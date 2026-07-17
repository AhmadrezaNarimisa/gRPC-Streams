# Realtime Distributed gRPC Streaming Server

A Kotlin and Spring Boot project for experimenting with high-concurrency, client-streaming gRPC connections backed by Redis Pub/Sub.

The server is designed around a target of **up to 100,000 concurrent streams**. That number is a configuration target, not a benchmark result: the practical limit depends on hardware, JVM settings, network conditions, message size, message rate, Redis capacity, and operating-system limits.

## Features

- Client-streaming gRPC endpoint for long-lived streams.
- Netty-based gRPC server on port `9090`.
- Configured maximum of `100,000` concurrent gRPC calls per connection.
- `128` Netty worker threads.
- Flow-control window and inbound message limits for high-throughput streaming.
- Bounded message queue between gRPC callbacks and Redis publishing.
- Redis Pub/Sub integration for distributing messages across application components.
- Reactive Spring WebFlux REST endpoint for publishing messages to Redis.
- Server-Sent Events endpoint for consuming Redis messages in real time.
- Kotlin test client with multiple channels, keepalive, retries, and virtual threads.
- Gradle task for running the test client from the same repository.

## Architecture

```text
                    client-streaming gRPC
  StreamingClient  ─────────────────────────▶  StreamingServer
       │                                             │
       │                                             │ bounded queue
       │                                             ▼
       │                                      MessageBroker
       │                                             │
       │                                             ▼
       │                                      Redis Pub/Sub
       │                                             │
       └─────────────────────────────────────────────┤
                                                     │
                         ┌───────────────────────────┴──────────────────────────┐
                         │                                                      │
                   REST publisher                                         SSE subscriber
              POST /api/message/send                                    GET /sse/stream
```

Each gRPC stream sends multiple `StreamRequest` messages and receives one `StreamResponse` when the client completes the stream. Every received message is placed in a bounded queue and published to the Redis channel `realtime-messages`.

## Technology stack

- Kotlin/JVM
- Java 23 toolchain
- Spring Boot and Spring WebFlux
- gRPC Java, gRPC Kotlin, and Netty
- Protocol Buffers
- Kotlin coroutines
- Redis Pub/Sub
- Gradle Wrapper

## Requirements

Install the following before running the project:

- JDK 23
- Docker Desktop with Docker Compose
- Git

Verify Java and Docker:

```bash
java -version
docker --version
docker compose version
```

The Gradle Wrapper downloads the required Gradle distribution and dependencies automatically.

## Run locally

### 1. Start Redis

From the repository root:

```bash
docker compose up -d redis
```

Check that Redis is running:

```bash
docker compose ps
```

Redis is exposed on `localhost:6379`.

### 2. Start the server

In a second terminal:

```bash
./gradlew bootRun
```

On Windows, use:

```powershell
gradlew.bat bootRun
```

The application starts:

- gRPC server: `localhost:9090`
- Spring WebFlux HTTP server: `localhost:8080`

The gRPC server prints stream statistics every five seconds. When a client connects, it reports messages such as:

```text
Streaming server started on port 9090
✅ New stream connected (ID: 1, Active: 1)
```

### 3. Start the test client

In a third terminal:

```bash
./gradlew runGrpcClient
```

The included client opens three streams across four gRPC channels and sends a message approximately every ten seconds. It prints client statistics every five seconds:

```text
Starting 3 streams across 4 channels...
Active streams: 3, messages: 3, rate: 0.60/s
```

The client is intentionally long-lived. Press `Ctrl+C` to stop it and close its channels.

## REST and SSE endpoints

### Publish a message through Redis

With the server running:

```bash
curl -X POST http://localhost:8080/api/message/send \
  -H 'Content-Type: text/plain' \
  --data 'hello from REST'
```

Expected response:

```text
success
```

### Subscribe to messages with SSE

In another terminal:

```bash
curl -N http://localhost:8080/sse/stream
```

Then publish a message using the REST command above. The SSE terminal receives an event similar to:

```text
data: hello from REST
```

Messages sent by the gRPC client are also published to Redis and can be observed through this SSE endpoint.

## Protocol

The gRPC contract is defined in [`src/main/proto/demo.proto`](src/main/proto/demo.proto).

The service exposes a client-streaming RPC:

```protobuf
rpc StreamData (stream StreamRequest) returns (StreamResponse);
```

Each request contains:

| Field | Type | Description |
| --- | --- | --- |
| `client_id` | `string` | Identifier for the sending client or stream |
| `data` | `string` | Application message payload |
| `timestamp` | `int64` | Message timestamp in milliseconds |

When the client completes a stream, the server returns:

| Field | Type | Description |
| --- | --- | --- |
| `success` | `bool` | Whether the stream completed successfully |
| `message` | `string` | Completion message |
| `total_messages_processed` | `int32` | Number of messages received on that stream |

Generated Java and Kotlin sources are created under `build/` during compilation. They are not committed to Git.

## High-concurrency configuration

The main gRPC tuning values are defined in [`StreamingServer.kt`](src/main/kotlin/org/example/realtimedistributedserver/grpc/StreamingServer.kt):

| Setting | Current value | Purpose |
| --- | ---: | --- |
| Maximum concurrent calls per connection | `100,000` | Upper bound configured for gRPC calls on a connection |
| Netty worker threads | `128` | Handles network and gRPC work |
| Flow-control window | `8 MiB` | Controls inbound flow control |
| Inbound message size | `1 MiB` | Maximum inbound gRPC message size |
| Inbound metadata size | `64 KiB` | Maximum inbound metadata size |
| Message queue capacity | `100,000` | Buffers messages before Redis publishing |
| Keepalive time | `30 seconds` | Server keepalive interval |
| Keepalive timeout | `10 seconds` | Keepalive response timeout |

The server uses a bounded queue. If the queue remains full for 100 milliseconds, the incoming message is dropped and a warning is printed. This behavior protects the server from unbounded memory growth when Redis or downstream consumers cannot keep up.

Before attempting a large load test, also review:

- File descriptor limits (`ulimit -n`).
- Available heap and native memory.
- CPU saturation and garbage-collection pauses.
- Network bandwidth and connection limits.
- Redis throughput and Pub/Sub subscriber count.
- Client-side channel, stream, and message-rate settings.

## Configure the test client

The sample client is in [`StreamingClient.kt`](src/main/kotlin/org/example/realtimedistributedserver/client/StreamingClient.kt).

The default entry point uses:

```kotlin
StreamingClient("localhost", 9090).use { client ->
    client.startMultipleStreams(
        streamCount = 3,
        messageIntervalMs = 10_000
    )
}
```

Change `streamCount` and `messageIntervalMs` for local experiments. For a serious load test, increase the load gradually and monitor CPU, memory, network traffic, Redis, and dropped-message warnings.

## Test and build commands

Compile the project:

```bash
./gradlew compileKotlin
```

Run the test suite:

```bash
./gradlew test
```

Build the application:

```bash
./gradlew build
```

Check all available tasks:

```bash
./gradlew tasks
```

The Spring Boot main class is explicitly configured so `bootRun` starts the server. The separate `runGrpcClient` task starts the client:

```kotlin
./gradlew bootRun
./gradlew runGrpcClient
```

## Stop local services

Stop the client and server with `Ctrl+C` in their respective terminals. Stop Redis with:

```bash
docker compose down
```

To also remove the Redis data volume:

```bash
docker compose down -v
```

The `-v` option deletes persisted local Redis data.

## Project structure

```text
.
├── build.gradle.kts
├── docker-compose.yml
├── gradlew
├── settings.gradle.kts
└── src
    ├── main
    │   ├── kotlin/org/example/realtimedistributedserver
    │   │   ├── client/StreamingClient.kt
    │   │   ├── config/RedisConfig.kt
    │   │   ├── Controller/MessageController.kt
    │   │   ├── Controller/SSEController.kt
    │   │   ├── grpc/StreamingServer.kt
    │   │   ├── model/StreamMessage.kt
    │   │   ├── Service/MessageBroker.kt
    │   │   └── RealtimeDistributedServerApplication.kt
    │   ├── proto/demo.proto
    │   └── resources/application.properties
    └── test
        └── kotlin/.../RealtimeDistributedServerApplicationTests.kt
```

## Troubleshooting

### `resolveMainClassName` reports two main classes

The project contains both the server and client entry points. The server main class is explicitly configured in `build.gradle.kts`. Make sure you are using the current version of the repository, then run:

```bash
./gradlew clean bootRun
```

### The client starts but the server shows no stream logs

Confirm that the server is listening on port `9090`:

```bash
lsof -nP -iTCP:9090 -sTCP:LISTEN
```

Start the server before the client and make sure the client uses `localhost:9090`. Restart any older server process before running `bootRun` again.

### Redis connection errors

Check Redis:

```bash
docker compose ps
docker compose logs redis
```

Restart it if necessary:

```bash
docker compose up -d redis
```

### Port already in use

The default ports are:

- `9090` for gRPC
- `8080` for Spring WebFlux
- `6379` for Redis

Stop the process using the port or change the corresponding application configuration before restarting.

## Current limitations

- The gRPC port is currently defined as a constant in `StreamingServer.kt`.
- The sample client uses plaintext gRPC and is intended for local development.
- The sample client is a functional test client, not a complete distributed load-generation system.
- No benchmark result is included in this repository; validate the 100k-stream target in the deployment environment.
- Authentication, authorization, TLS, metrics export, tracing, and production-grade configuration management are not included yet.

## License

No license has been declared for this repository yet. Add a license before publishing it for external use.
