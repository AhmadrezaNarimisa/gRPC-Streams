package org.example.realtimedistributedserver.model

data class StreamMessage(
    val data: String,
    val timestamp: Long = System.currentTimeMillis()
)
