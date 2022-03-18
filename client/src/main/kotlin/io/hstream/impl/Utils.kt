package io.hstream.impl

import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.hstream.HStreamDBClientException
import io.hstream.internal.HStreamApiGrpcKt.HStreamApiCoroutineStub
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext

val logger: Logger = LoggerFactory.getLogger("io.hstream.impl.Utils")

suspend fun <Resp> unaryCallWithCurrentUrlsCoroutine(serverUrls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    // Note: A failed grpc call can throw both 'StatusException' and 'StatusRuntimeException'.
    //       This function is for handling them.
    suspend fun handleGRPCException(i: Int, e: Throwable) {
        logger.error("call unary rpc with url [{}] error", serverUrls[i], e)
        val status = Status.fromThrowable(e)
        if (status.code == Status.UNAVAILABLE.code) {
            if (i == serverUrls.size - 1) {
                throw HStreamDBClientException(e)
            } else {
                logger.info("unary rpc will be retried with url [{}]", serverUrls[i + 1])
                delay(DefaultSettings.REQUEST_RETRY_INTERVAL_SECONDS * 1000)
                return
            }
        } else {
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
        logger.info("refreshClusterInfo gets new urls [{}]", serverUrls)
        return@unaryCallWithCurrentUrlsCoroutine newServerUrls
    }
}

suspend fun <Resp> unaryCallCoroutine(urlsRef: AtomicReference<List<String>>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    // Note: A failed grpc call can throw both 'StatusException' and 'StatusRuntimeException'.
    //       This function is for handling them.
    suspend fun handleGRPCException(urls: List<String>, e: Throwable): Resp {
        logger.error("unary rpc error with url [{}]", urls[0], e)
        val status = Status.fromThrowable(e)
        if (status.code == Status.UNAVAILABLE.code && urls.size > 1) {
            val newServerUrls = refreshClusterInfo(urls.subList(1, urls.size), channelProvider)
            urlsRef.set(newServerUrls)
            return unaryCallWithCurrentUrlsCoroutine(urlsRef.get(), channelProvider, call)
        } else {
            throw HStreamDBClientException(e)
        }
    }

    val urls = urlsRef.get()
    check(urls.isNotEmpty())

    logger.debug("unary rpc with urls [{}]", urls)

    try {
        return call(HStreamApiCoroutineStub(channelProvider.get(urls[0])))
    } catch (e: StatusException) {
        return handleGRPCException(urls, e)
    } catch (e: StatusRuntimeException) {
        return handleGRPCException(urls, e)
    }
}

fun <Resp> unaryCallAsync(urlsRef: AtomicReference<List<String>>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): CompletableFuture<Resp> {
    return futureForIO { unaryCallCoroutine(urlsRef, channelProvider, call) }
}

// warning: this method will block current thread. Do not call this in suspend functions, use unaryCallCoroutine instead!
fun <Resp> unaryCallBlocked(urlsRef: AtomicReference<List<String>>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    return futureForIO(MoreExecutors.directExecutor().asCoroutineDispatcher()) { unaryCallCoroutine(urlsRef, channelProvider, call) }.join()
}

fun <Resp> unaryCallWithCurrentUrlsAsync(urls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): CompletableFuture<Resp> {
    return futureForIO { unaryCallWithCurrentUrlsCoroutine(urls, channelProvider, call) }
}

fun <Resp> unaryCallWithCurrentUrls(urls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    return unaryCallWithCurrentUrlsAsync(urls, channelProvider, call).join()
}

@OptIn(DelicateCoroutinesApi::class)
fun <T> futureForIO(context: CoroutineContext = Dispatchers.Default, block: suspend CoroutineScope.() -> T): CompletableFuture<T> {
    return GlobalScope.future(context = context, block = block)
}
