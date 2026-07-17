package org.example.realtimedistributedserver.grpc

import io.grpc.Server
import io.grpc.Status
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.example.realtimedistributedserver.Service.MessageBroker
import org.example.realtimedistributedserver.StreamRequest
import org.example.realtimedistributedserver.StreamResponse
import org.example.realtimedistributedserver.StreamingServiceGrpc
import org.example.realtimedistributedserver.model.StreamMessage
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class StreamingServer(private val messageBroker: MessageBroker) {
    private val server: Server
    private val activeStreams = AtomicInteger(0)
    private val totalStreamsCreated = AtomicInteger(0)
    private val streamsSinceLastReport = AtomicInteger(0)

    private val serverDispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private val streamScope = CoroutineScope(serverDispatcher + SupervisorJob())

    companion object {
        private const val PORT = 9090
        private const val MAX_CONCURRENT_STREAMS = 100000
        private const val WORKER_THREADS = 128
        private const val MONITORING_INTERVAL_SECONDS = 5L

    }

    init {
        val bossGroup: EventLoopGroup = NioEventLoopGroup(4)
        val workerGroup: EventLoopGroup = NioEventLoopGroup(WORKER_THREADS)

        streamScope.launch { monitorStreams() }
        val builder = NettyServerBuilder.forPort(PORT)
            .channelType(NioServerSocketChannel::class.java)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .executor(serverDispatcher.asExecutor())
            .maxConcurrentCallsPerConnection(MAX_CONCURRENT_STREAMS)
            .maxInboundMessageSize(1024 * 1024)
            .maxInboundMetadataSize(1024 * 64)
            .flowControlWindow(1024 * 1024 * 8)

// Add these new configurations
            .permitKeepAliveTime(20, TimeUnit.SECONDS)  // Allow keepalive after 20 seconds
            .permitKeepAliveWithoutCalls(true)  // Allow keepalive even when no RPCs are in flight
            .keepAliveTime(30, TimeUnit.SECONDS)  // Server keepalive time
            .keepAliveTimeout(10, TimeUnit.SECONDS)  // Server keepalive timeout
            .addService(StreamingServiceImpl(streamScope, activeStreams, totalStreamsCreated, streamsSinceLastReport, messageBroker))

        server = builder.build()
    }

    private suspend fun monitorStreams() {
        while (true) {
            delay(MONITORING_INTERVAL_SECONDS.seconds)
            val currentActive = activeStreams.get()
            val newStreams = streamsSinceLastReport.getAndSet(0)
            val total = totalStreamsCreated.get()

            println("""
            |=== Stream Statistics ===
            |Active Streams: $currentActive
            |New Streams (last ${MONITORING_INTERVAL_SECONDS}s): $newStreams
            |Total Streams Created: $total
            |Stream Creation Rate: ${newStreams / MONITORING_INTERVAL_SECONDS}/s
            |Memory Usage: ${Runtime.getRuntime().totalMemory() / 1024 / 1024}MB
            |========================
            """.trimMargin())
        }
    }
    private class StreamingServiceImpl(
        scope: CoroutineScope,
        private val activeStreams: AtomicInteger,
        private val totalStreamsCreated: AtomicInteger,
        private val streamsSinceLastReport: AtomicInteger,
        private val messageBroker: MessageBroker
    ) : StreamingServiceGrpc.StreamingServiceImplBase() {

        // to create bridge between redis and grpc
        private val messageBuffer = ArrayBlockingQueue<StreamMessage>(QUEUE_CAPACITY)

        init {
            // Start message processing coroutine
            scope.launch(Dispatchers.Default) {
                processMessagesForRedis()
            }
        }

        private suspend fun processMessagesForRedis() {
            while (currentCoroutineContext().isActive) {
                try {
                    // Process messages in batches for efficiency
                    val messageBatch = mutableListOf<StreamMessage>()
                    messageBuffer.drainTo(messageBatch, 100)

                    if (messageBatch.isNotEmpty()) {
                        messageBatch.forEach { message ->
                            messageBroker.sendMessage(message.data)
                        }
                    } else {
                        delay(10) // Prevent CPU spinning
                    }
                } catch (e: Exception) {
                    println("Redis publishing error: ${e.message}")
                    delay(100)
                }
            }
        }

        // this is responsible for handling incoming client streams
        override fun streamData(
            responseObserver: StreamObserver<StreamResponse>
        ): StreamObserver<StreamRequest> {
            // Enforce connection limits
            if (activeStreams.get() >= MAX_CONCURRENT_STREAMS) {
                println("❌ Connection rejected: Max streams ($MAX_CONCURRENT_STREAMS) reached")
                throw Status.RESOURCE_EXHAUSTED
                    .withDescription("Max streams reached")
                    .asRuntimeException()
            }

            val streamId = totalStreamsCreated.incrementAndGet()
            streamsSinceLastReport.incrementAndGet()
            val currentActive = activeStreams.incrementAndGet()
            var messageCount = 0
            var streamClosed = false

            println("✅ New stream connected (ID: $streamId, Active: $currentActive)")

            return object : StreamObserver<StreamRequest> {
                override fun onNext(request: StreamRequest) {
                    messageCount++
                    // Convert gRPC request to domain message
                    val message = StreamMessage(
                        data = request.data,
                        timestamp = request.timestamp
                    )

                    // Add to processing queue with backpressure
                    if (!messageBuffer.offer(message, 100, TimeUnit.MILLISECONDS)) {
                        println("⚠️ Buffer full for stream $streamId - message dropped")
                    }
                }

                override fun onError(error: Throwable) {
                    println("❌ Error in stream $streamId: ${error.message}")
                    cleanupStream()
                }

                // this method will get invoked once you are done with the streams. means you want to close it.
                override fun onCompleted() {
                    // Send final success response
                    val response = StreamResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Stream processed successfully")
                        .setTotalMessagesProcessed(messageCount)
                        .build()

                    responseObserver.onNext(response)
                    responseObserver.onCompleted()

                    println("✅ Stream $streamId completed. Processed $messageCount messages")
                    cleanupStream()
                }

                private fun cleanupStream() {
                    if (streamClosed) return
                    streamClosed = true
                    val remainingStreams = activeStreams.decrementAndGet()
                    println("❎ Stream disconnected (ID: $streamId, Remaining: $remainingStreams)")
                }
            }
        }

        companion object {
            private const val QUEUE_CAPACITY = 100000
        }
    }

    // Start method to start streaming server
    fun start() {
        server.start()
        println("""
            |Streaming server started on port $PORT
            |Maximum concurrent streams: $MAX_CONCURRENT_STREAMS
            |Worker threads: $WORKER_THREADS
            |Monitoring interval: ${MONITORING_INTERVAL_SECONDS}s
        """.trimIndent())
    }

}
