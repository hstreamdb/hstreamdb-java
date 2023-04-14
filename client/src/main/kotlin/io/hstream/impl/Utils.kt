package io.hstream.impl

import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.hstream.HServerException.tryToHServerException
import io.hstream.HStreamDBClientException
import io.hstream.internal.HStreamApiGrpcKt.HStreamApiCoroutineStub
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext

val logger: Logger = LoggerFactory.getLogger("io.hstream.impl.Utils")

suspend fun <Resp> unaryCallWithCurrentUrlsCoroutine(serverUrls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    // Note: A failed grpc call can throw both 'StatusException' and 'StatusRuntimeException'.
    //       This function is for handling them.
    suspend fun handleGRPCException(i: Int, e: Throwable) {
        logger.error("call unary rpc with url [{}] error, msg:{}", serverUrls[i], e.message)
        val status = Status.fromThrowable(e)
        if (status.code == Status.UNAVAILABLE.code) {
            if (i == serverUrls.size - 1) {
                val hServerErr = tryToHServerException(status.description)
                if (hServerErr != null) {
                    throw hServerErr
                }
                throw HStreamDBClientException(e)
            } else {
                logger.info("unary rpc will be retried with url [{}]", serverUrls[i + 1])
                delay(DefaultSettings.REQUEST_RETRY_INTERVAL_SECONDS * 1000)
                return
            }
        } else {
            val hServerErr = tryToHServerException(status.description)
            if (hServerErr != null) {
                throw hServerErr
            }
            throw HStreamDBClientException(e)
        }
    }

    check(serverUrls.isNotEmpty())
    logger.debug("call unaryCallWithCurrentUrlsCoroutine with urls [{}]", serverUrls)
    for (i in serverUrls.indices) {
        val stub = HStreamApiCoroutineStub(channelProvider.get(serverUrls[i]))
        try {
            return call(stub)
        } catch (e: StatusException) {
            handleGRPCException(i, e)
        } catch (e: StatusRuntimeException) {
            handleGRPCException(i, e)
        }
    }

    throw IllegalStateException("should not reach here")
}

suspend fun refreshClusterInfo(serverUrls: List<String>, channelProvider: ChannelProvider): List<String> {
    logger.info("ready to refresh cluster info with urls [{}]", serverUrls)
    return unaryCallWithCurrentUrlsCoroutine(serverUrls, channelProvider) {
        val resp = it.describeCluster(Empty.getDefaultInstance())
        val serverNodes = resp.serverNodesList
        val newServerUrls: ArrayList<String> = ArrayList(serverNodes.size)
        for (serverNode in serverNodes) {
            val host = serverNode.host
            val port = serverNode.port
            newServerUrls.add("$host:$port")
        }
        logger.info("refreshClusterInfo gets new urls [{}]", newServerUrls)
        return@unaryCallWithCurrentUrlsCoroutine newServerUrls
    }
}

suspend fun <Resp> unaryCallCoroutine(
    urlsRef: AtomicReference<List<String>>,
    channelProvider: ChannelProvider,
    timeoutMs: Long,
    call: suspend (stub: HStreamApiCoroutineStub) -> Resp
): Resp {
    // Note: A failed grpc call can throw both 'StatusException' and 'StatusRuntimeException'.
    //       This function is for handling them.
    suspend fun handleGRPCException(urls: List<String>, e: Throwable): Resp {
        logger.error("unary rpc error with url [{}], msg:{}", urls[0], e.message)
        val status = Status.fromThrowable(e)
        if (status.code == Status.UNAVAILABLE.code && urls.size > 1) {
            val newServerUrls = refreshClusterInfo(urls.subList(1, urls.size), channelProvider)
            urlsRef.set(newServerUrls)
            return unaryCallWithCurrentUrlsCoroutine(urlsRef.get(), channelProvider, call)
        } else {
            val hServerErr = tryToHServerException(status.description)
            if (hServerErr != null) {
                throw hServerErr
            }
            throw HStreamDBClientException(e)
        }
    }

    val urls = urlsRef.get()
    check(urls.isNotEmpty())

    logger.debug("unary rpc with urls [{}]", urls)

    return try {
        call(HStreamApiCoroutineStub(channelProvider.get(urls[0])).withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS))
    } catch (e: StatusException) {
        handleGRPCException(urls, e)
    } catch (e: StatusRuntimeException) {
        handleGRPCException(urls, e)
    }
}

fun <Resp> unaryCallAsync(
    urlsRef: AtomicReference<List<String>>,
    channelProvider: ChannelProvider,
    timeoutMs: Long,
    call: suspend (stub: HStreamApiCoroutineStub) -> Resp
): CompletableFuture<Resp> {
    return futureForIO { unaryCallCoroutine(urlsRef, channelProvider, timeoutMs, call) }
}

// warning: this method will block current thread. Do not call this in suspend functions, use unaryCallCoroutine instead!
fun <Resp> unaryCallBlocked(
    urlsRef: AtomicReference<List<String>>,
    channelProvider: ChannelProvider,
    timeoutMs: Long,
    call: suspend (stub: HStreamApiCoroutineStub) -> Resp
): Resp {
    return runBlocking(MoreExecutors.directExecutor().asCoroutineDispatcher()) {
        unaryCallCoroutine(
            urlsRef,
            channelProvider,
            timeoutMs,
            call
        )
    }
}

fun <Resp> unaryCallWithCurrentUrlsAsync(urls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): CompletableFuture<Resp> {
    return futureForIO { unaryCallWithCurrentUrlsCoroutine(urls, channelProvider, call) }
}

fun <Resp> unaryCallWithCurrentUrls(urls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    return runBlocking { unaryCallWithCurrentUrlsCoroutine(urls, channelProvider, call) }
}

@OptIn(DelicateCoroutinesApi::class)
fun <T> futureForIO(context: CoroutineContext = Dispatchers.Default, block: suspend CoroutineScope.() -> T): CompletableFuture<T> {
    return GlobalScope.future(context = context, block = block)
}
