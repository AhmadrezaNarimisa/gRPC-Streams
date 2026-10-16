package org.example.realtimedistributedserver

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RealtimeDistributedServerApplication

fun main(args: Array<String>) {
    runApplication<RealtimeDistributedServerApplication>(*args)
}
