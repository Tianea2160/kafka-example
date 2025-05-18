package org.tianea.kafkaexample.service

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaProducerService(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun sendMessage(topic: String, message: String) {
        logger.info("Sending message to topic: $topic, message: $message")
        kafkaTemplate.send(topic, message)
            .whenComplete { result, ex ->
                if (ex == null) {
                    logger.info("Message sent successfully to topic: ${result.recordMetadata.topic()}, " +
                            "partition: ${result.recordMetadata.partition()}, " +
                            "offset: ${result.recordMetadata.offset()}")
                } else {
                    logger.error("Failed to send message", ex)
                }
            }
    }
}
