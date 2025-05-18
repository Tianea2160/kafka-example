package org.tianea.kafkaexample.service

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["example-topic"], groupId = "kafka-example-group")
    fun listen(message: String) {
        logger.info("Received message: $message")
    }
}
