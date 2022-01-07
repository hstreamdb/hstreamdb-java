package io.hstream.impl

import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.Empty
import io.grpc.Status
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

val logger: Logger = LoggerFactory.getLogger("kt-coroutine-utils")

suspend fun <Resp> unaryCallWithCurrentUrlsCoroutine(serverUrls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    check(serverUrls.isNotEmpty())
    logger.info("unaryCallWithCurrentUrl urls are {}", serverUrls)
    for (i in serverUrls.indices) {
        val stub = HStreamApiCoroutineStub(channelProvider.get(serverUrls[i]))
        try {
            return call(stub)
        } catch (e: StatusRuntimeException) {
            logger.warn("unaryCallWithCurrentUrl with url {} error", serverUrls[i], e)
            val status = Status.fromThrowable(e)
            if (status.code == Status.UNAVAILABLE.code) {
                if (i == serverUrls.size - 1) {
                    throw HStreamDBClientException(e)
                } else {
                    delay(DefaultSettings.REQUEST_RETRY_INTERVAL_SECONDS * 1000)
                    continue
                }
            } else {
                throw HStreamDBClientException(e)
            }
        }
    }

    throw IllegalStateException("should not reach here")
}

suspend fun refreshClusterInfo(serverUrls: List<String>, channelProvider: ChannelProvider): List<String> {
    logger.info("refresh cluster info with urls: {}", serverUrls)
    return unaryCallWithCurrentUrlsCoroutine(serverUrls, channelProvider) {
        val resp = it.describeCluster(Empty.getDefaultInstance())
        val serverNodes = resp.serverNodesList
        val newServerUrls: ArrayList<String> = ArrayList(serverNodes.size)
        for (serverNode in serverNodes) {
            val host = serverNode.host
            val port = serverNode.port
            newServerUrls.add("$host:$port")
        }
        return@unaryCallWithCurrentUrlsCoroutine newServerUrls
    }
}

suspend fun <Resp> unaryCallCoroutine(urlsRef: AtomicReference<List<String>>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    val urls = urlsRef.get()
    check(urls.isNotEmpty())

    logger.info("unary call, urls are {}", urls)

    try {
        return call(HStreamApiCoroutineStub(channelProvider.get(urls[0])))
    } catch (e: StatusRuntimeException) {
        logger.warn("unary call error for url: {}", urls[0], e)
        val status = Status.fromThrowable(e)
        if (status.code == Status.UNAVAILABLE.code && urls.size > 1) {
            logger.info("before refreshClusterInfo, urls are {}", urls)
            val newServerUrls = refreshClusterInfo(urls.subList(1, urls.size), channelProvider)
            urlsRef.set(newServerUrls)
            logger.info("after refreshClusterInfo, urls are {}", urlsRef.get())
            return unaryCallWithCurrentUrlsCoroutine(urlsRef.get(), channelProvider, call)
        } else {
            throw HStreamDBClientException(e)
        }
    }
}

fun <Resp> unaryCallAsync(urlsRef: AtomicReference<List<String>>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): CompletableFuture<Resp> {
    return futureForIO { unaryCallCoroutine(urlsRef, channelProvider, call) }
}

// warning: this method will block current thread. Don not call this in suspend functions, use unaryCallCoroutine instead!
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
