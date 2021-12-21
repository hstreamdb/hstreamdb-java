package io.hstream.impl

import com.google.protobuf.Empty
import io.hstream.internal.HStreamApiGrpcKt.HStreamApiCoroutineStub
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference

suspend fun <Resp> unaryCallWithCurrentUrlsCoroutine(serverUrls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    for (i in serverUrls.indices) {
        val stub = HStreamApiCoroutineStub(channelProvider.get(serverUrls[i]))
        try {
            return call(stub)
        } catch (e: Exception) {
            if (i == serverUrls.size - 1) {
                throw e
            }

            delay(1000)
        }
    }

    throw IllegalStateException("should not reach here")
}

suspend fun refreshClusterInfo(serverUrls: List<String>, channelProvider: ChannelProvider): List<String> {
    return unaryCallWithCurrentUrlsCoroutine(serverUrls, channelProvider) {
        val resp = it.describeCluster(Empty.getDefaultInstance())
        val serverNodes = resp.serverNodesList
        val newServerUrls: ArrayList<String> = ArrayList(serverNodes.size)
        for (serverNode in serverNodes) {
            val host = serverNode.host;
            val port = serverNode.port;
            newServerUrls.add("$host:$port");
        }
        return@unaryCallWithCurrentUrlsCoroutine newServerUrls
    }
}

suspend fun <Resp> unaryCallCoroutine(urlsRef: AtomicReference<List<String>>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    val urls = urlsRef.get()
    check(urls.isNotEmpty())

    try {
        return call(HStreamApiCoroutineStub(channelProvider.get(urls[0])))
    } catch (e: Exception) {
        if (urls.size > 1) {
            val newServerUrls = refreshClusterInfo(urls.subList(1, urls.size), channelProvider)
            urlsRef.compareAndSet(urls, newServerUrls)
            return unaryCallWithCurrentUrlsCoroutine(urlsRef.get(), channelProvider, call)
        } else {
            throw e
        }

    }
}

@OptIn(DelicateCoroutinesApi::class)
fun <Resp> unaryCallAsync(urlsRef: AtomicReference<List<String>>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): CompletableFuture<Resp> {
    return GlobalScope.future { unaryCallCoroutine(urlsRef, channelProvider, call) }
}


fun <Resp> unaryCall(urls: AtomicReference<List<String>>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    return unaryCallAsync(urls, channelProvider, call).join()
}

@OptIn(DelicateCoroutinesApi::class)
fun <Resp> unaryCallWithCurrentUrlsAsync(urls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): CompletableFuture<Resp> {
    return GlobalScope.future { unaryCallWithCurrentUrlsCoroutine(urls, channelProvider, call) }
}

fun <Resp> unaryCallWithCurrentUrls(urls: List<String>, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): Resp {
    return unaryCallWithCurrentUrlsAsync(urls, channelProvider, call).join()
}

@OptIn(DelicateCoroutinesApi::class)
fun <Resp> unaryCallSimpleAsync(url: String, channelProvider: ChannelProvider, call: suspend (stub: HStreamApiCoroutineStub) -> Resp): CompletableFuture<Resp> {
    return GlobalScope.future { call(HStreamApiCoroutineStub(channelProvider.get(url)))}
}
