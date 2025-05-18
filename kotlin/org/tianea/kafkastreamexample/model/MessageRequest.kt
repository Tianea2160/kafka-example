package org.tianea.kafkastreamexample.model

data class MessageRequest(
    val key: String,
    val message: String
)