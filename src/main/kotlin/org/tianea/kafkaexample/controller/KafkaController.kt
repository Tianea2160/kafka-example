package org.tianea.kafkaexample.controller

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.tianea.kafkaexample.model.Message
import org.tianea.kafkaexample.service.KafkaProducerService

@RestController
@RequestMapping("/api/kafka")
class KafkaController(
    private val kafkaProducerService: KafkaProducerService
) {

    @PostMapping("/publish")
    fun publish(@RequestBody message: Message): String {
        kafkaProducerService.sendMessage("example-topic", message.content)
        return "Message sent to the Kafka topic successfully"
    }
}
