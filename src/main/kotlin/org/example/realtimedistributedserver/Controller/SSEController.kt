package org.example.realtimedistributedserver.Controller

import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactor.asFlux
import org.example.realtimedistributedserver.Service.MessageBroker
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux

@RestController
@RequestMapping("/sse")
class SSEController(private val messageBroker: MessageBroker) {

    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
     fun stream(): Flux<String> {
        return messageBroker
            .messageFlow
            .map { message -> """
                data: $message 
            """.trimIndent() }
            .asFlux()
    }
}