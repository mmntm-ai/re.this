package eu.vendeli.rethis.core

import eu.vendeli.rethis.ReThis
import eu.vendeli.rethis.types.common.RConnection
import eu.vendeli.rethis.types.common.rConnection
import eu.vendeli.rethis.types.common.toArgument
import eu.vendeli.rethis.utils.IO_OR_UNCONFINED
import eu.vendeli.rethis.utils.writeRedisValue
import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.network.tls.*
import io.ktor.util.logging.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.onFailure
import kotlinx.io.Buffer
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

internal class ConnectionPool(
    internal val client: ReThis,
) {
    internal val logger = KtorSimpleLogger("eu.vendeli.rethis.ConnectionPool")

    @OptIn(ExperimentalCoroutinesApi::class)
    internal val isEmpty: Boolean get() = connections.isEmpty

    private val poolJob = Job(client.rootJob)
    private val poolScope = CoroutineScope(
        client.cfg.connectionConfiguration.dispatcher + poolJob + CoroutineName("ReThis|ConnectionPool"),
    )
    private val connections = Channel<RConnection>(client.cfg.connectionConfiguration.poolSize)
    private val selector = SelectorManager(poolScope.coroutineContext)

    private var connectionAttempts = 0
    private var activeConnections = 0

    init {
        poolScope.launch { prepare() }
    }

    internal suspend fun createConn(): RConnection {
        logger.trace { "Creating connection to ${client.address}" }
        val conn = withTimeout(client.cfg.connectionTimeout) {
            aSocket(selector)
                .tcp()
                .connect(client.address.socket) {
                    client.cfg.socketConfiguration.run {
                        timeout?.let { socketTimeout = it }
                        linger?.let { lingerSeconds = it }
                        this@connect.keepAlive = keepAlive
                        this@connect.noDelay = noDelay
                    }
                }.let { socket ->
                    client.cfg.tlsConfig?.let {
                        socket.tls(selector.coroutineContext, it)
                    } ?: socket
                }.rConnection()
        }

        val reqBuffer = Buffer()
        var requests = 0

        if (client.cfg.auth != null) client.cfg.auth?.run {
            logger.trace { "Authenticating to ${client.address}." }
            reqBuffer.writeRedisValue(listOfNotNull("AUTH".toArgument(), username?.toArgument(), password.toArgument()))
            requests++
        }

        client.cfg.db?.takeIf { it > 0 }?.let {
            logger.trace { "Selecting database $it to ${client.address}." }
            reqBuffer.writeRedisValue(listOf("SELECT".toArgument(), it.toArgument()))
            requests++
        }

        reqBuffer.writeRedisValue(listOf("HELLO".toArgument(), client.protocol.literal.toArgument()))
        requests++

        logger.trace { "Sending connection establishment requests ($requests)" }
        conn.sendRequest(reqBuffer).readBatchResponse(requests)

        return conn
    }

    @Suppress("OPT_IN_USAGE")
    fun prepare() = poolScope.launch(Dispatchers.IO_OR_UNCONFINED) {
        logger.info("Filling ConnectionPool with connections (${client.cfg.connectionConfiguration.poolSize})")
        if (connections.isEmpty) repeat(client.cfg.connectionConfiguration.poolSize) {
            connectionAttempts++
            launch {
                runCatching { createConn() }
                    .onSuccess { conn ->
                        connectionAttempts--
                        activeConnections++
                        connections.trySend(conn)
                    }
                    .onFailure { ex ->
                        connectionAttempts--
                        logger.warn("Failed to create connection during pool preparation", ex)
                    }
            }
        }
    }

    suspend fun acquire(): RConnection {
        // First try to get an existing connection
        val result = connections.tryReceive()
        if (result.isSuccess) {
            return result.getOrThrow()
        }

        // If pool is empty but we can create more connections, create one
        poolScope.launch(Dispatchers.IO_OR_UNCONFINED) {
            if (activeConnections + connectionAttempts < client.cfg.connectionConfiguration.poolSize) {
                logger.trace { "Pool empty, creating new connection (active: $activeConnections, attempts: $connectionAttempts, poolSize: ${client.cfg.connectionConfiguration.poolSize})" }
                connectionAttempts++
                try {
                    val conn = createConn()
                    connectionAttempts--
                    activeConnections++
                    connections.trySend(conn)
                } catch (ex: Throwable) {
                    connectionAttempts--
                    throw ex
                }
            }
        }

        // Wait for a connection to become available
        return withTimeout(client.cfg.poolTimeout) { connections.receive() }
    }

    fun release(connection: RConnection) {
        handle(connection)
    }

    private fun handle(connection: RConnection) = poolScope.launch(Dispatchers.IO_OR_UNCONFINED) {
        logger.trace { "Releasing connection ${connection.socket}" }
        if (connection.input.isClosedForRead) { // connection is corrupted
            logger.warn("Connection ${connection.socket} is unexpectedly closed, refilling")
            activeConnections--
            launch {
                connection.socket.close()
                refill()
            }
        } else {
            connections.trySend(connection).onFailure {
                logger.warn("Pool is full, closing connection ${connection.socket}")
                activeConnections--
                connection.socket.close()
            }
        }
    }

    private suspend fun refill() {
        val cfg = client.cfg.connectionConfiguration
        if (cfg.reconnectAttempts <= 0) return
        var attempt = 0
        var ex: Throwable? = null

        while (attempt < cfg.reconnectAttempts) {
            attempt++
            logger.trace { "Refilling ConnectionPool ($activeConnections + $connectionAttempts / ${client.cfg.connectionConfiguration.poolSize}). Attempt $attempt" }
            connectionAttempts++
            runCatching { createConn() }
                .onSuccess { conn ->
                    connectionAttempts--
                    activeConnections++
                    connections.send(conn)
                    logger.trace { "Connection refilled with $conn" }
                    return
                }.onFailure {
                    connectionAttempts--
                    if (ex != null) ex.addSuppressed(it) else ex = it
                }

            logger.debug("Connection refill failed, remaining attempts: ${cfg.reconnectAttempts - attempt}")
            delay(attempt * cfg.reconnectDelay)
        }

        val logMsg = "Connection refills failed, maximum attempts reached"
        if (ex == null) logger.warn(logMsg)
        else logger.warn(logMsg, ex)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun disconnect() {
        logger.debug("Disconnecting from Redis")
        connections.close()
        while (!connections.isEmpty) {
            connections.receive().socket.close()
            activeConnections--
        }
        connectionAttempts = 0
        activeConnections = 0
    }
}

@OptIn(ExperimentalContracts::class)
internal suspend inline fun <R> ConnectionPool.use(block: (RConnection) -> R): R {
    contract {
        callsInPlace(block, InvocationKind.EXACTLY_ONCE)
    }
    val connection = withTimeout(client.cfg.poolTimeout) { acquire() }
    logger.trace { "Using ${connection.socket} for request" }
    return try {
        block(connection)
    } catch (e: Throwable) {
        throw e
    } finally {
        release(connection)
    }
}
