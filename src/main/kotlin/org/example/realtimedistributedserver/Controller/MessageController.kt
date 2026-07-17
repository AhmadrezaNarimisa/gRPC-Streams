package org.example.realtimedistributedserver.Controller

import org.example.realtimedistributedserver.Service.MessageBroker
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

// publish message to redis manually
// expecting gprc server to get message in realtime from grpc client

@RestController
@RequestMapping("/api/message")
class MessageController(private val messageBroker: MessageBroker) {

    @PostMapping("/send")
    suspend fun sendMessage(@RequestBody message: String): ResponseEntity<String> {
    messageBroker.sendMessage(message = message)
        return ResponseEntity.ok("success")
    }

}