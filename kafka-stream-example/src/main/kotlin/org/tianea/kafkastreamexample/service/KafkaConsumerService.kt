package org.tianea.kafkastreamexample.service

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import org.springframework.kafka.support.KafkaHeaders

@Service
class KafkaConsumerService {

    private val logger = LoggerFactory.getLogger(KafkaConsumerService::class.java)

    @KafkaListener(
        topics = ["\${kafka.output-topic:output-topic}"],
        groupId = "output-consumer-group"
    )
    fun listen(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) key: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        logger.info("처리된 메시지 수신: {} - {} [partition: {}, offset: {}]", key, message, partition, offset)
    }
}