package org.example.realtimedistributedserver.client

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.example.realtimedistributedserver.StreamRequest
import org.example.realtimedistributedserver.StreamResponse
import org.example.realtimedistributedserver.StreamingServiceGrpc
import java.io.Closeable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

class StreamingClient(
    private val host: String,
    private val port: Int
) : Closeable {
    companion object {
        private const val CHANNEL_COUNT = 4
        private const val MONITORING_INTERVAL_MS = 5_000L
    }

    private val channels: List<ManagedChannel> = (0 until CHANNEL_COUNT).map {
        ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(1024 * 1024)
            .keepAliveTime(60, TimeUnit.SECONDS)
            .keepAliveTimeout(20, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .enableRetry()
            .maxRetryAttempts(5)
            .build()
    }

    private val stubs = channels.map(StreamingServiceGrpc::newStub)
    private val clientDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
    private val clientScope = CoroutineScope(clientDispatcher + SupervisorJob())
    private val activeStreams = AtomicInteger()
    private val totalMessagesSent = AtomicInteger()
    private val random = Random(System.currentTimeMillis())

    init {
        clientScope.launch {
            monitorStreams()
        }
    }

    suspend fun startMultipleStreams(streamCount: Int, messageIntervalMs: Long = 1_000) = coroutineScope {
        println("Starting $streamCount streams across ${channels.size} channels...")

        repeat(streamCount) { streamId ->
            launch(clientDispatcher) {
                startSingleStream(streamId, messageIntervalMs, stubs[streamId % stubs.size])
            }
            delay(50)
        }
    }

    private suspend fun startSingleStream(
        streamId: Int,
        messageIntervalMs: Long,
        stub: StreamingServiceGrpc.StreamingServiceStub
    ) {
        val streamActive = AtomicInteger(1)
        activeStreams.incrementAndGet()

        try {
            val responseObserver = object : StreamObserver<StreamResponse> {
                override fun onNext(response: StreamResponse) {
                    println("Stream $streamId received: ${response.message}")
                }

                override fun onError(error: Throwable) {
                    println("Stream $streamId error: ${error.message}")
                    if (streamActive.getAndSet(0) == 1) activeStreams.decrementAndGet()
                }

                override fun onCompleted() {
                    println("Stream $streamId completed")
                    if (streamActive.getAndSet(0) == 1) activeStreams.decrementAndGet()
                }
            }

            val requestObserver = stub.streamData(responseObserver)
            while (streamActive.get() == 1) {
                requestObserver.onNext(
                    StreamRequest.newBuilder()
                        .setClientId("client-$streamId")
                        .setData("Message from stream $streamId")
                        .setTimestamp(System.currentTimeMillis())
                        .build()
                )
                totalMessagesSent.incrementAndGet()
                val jitter = (random.nextDouble() * 0.2 * messageIntervalMs).toLong()
                delay(messageIntervalMs + jitter)
            }
            requestObserver.onCompleted()
        } catch (error: Exception) {
            println("Stream $streamId failed: ${error.message}")
            if (streamActive.getAndSet(0) == 1) activeStreams.decrementAndGet()
        }
    }

    private suspend fun monitorStreams() {
        var previousMessageCount = 0
        while (true) {
            delay(MONITORING_INTERVAL_MS)
            val currentMessageCount = totalMessagesSent.get()
            val messageRate = (currentMessageCount - previousMessageCount) /
                (MONITORING_INTERVAL_MS / 1_000.0)
            previousMessageCount = currentMessageCount
            println("Active streams: ${activeStreams.get()}, messages: $currentMessageCount, rate: %.2f/s"
                .format(messageRate))
        }
    }

    override fun close() {
        clientScope.cancel()
        clientDispatcher.close()
        channels.forEach { channel ->
            channel.shutdown()
            channel.awaitTermination(5, TimeUnit.SECONDS)
        }
    }
}

suspend fun main() {
    StreamingClient("localhost", 9090).use { client ->
        client.startMultipleStreams(streamCount = 3, messageIntervalMs = 10_000)
        delay(Long.MAX_VALUE)
    }
}
