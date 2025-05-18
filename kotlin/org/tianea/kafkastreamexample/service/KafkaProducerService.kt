package org.tianea.kafkastreamexample.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

@Service
class KafkaProducerService(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(KafkaProducerService::class.java)

    @Value("\${kafka.input-topic:input-topic}")
    private lateinit var inputTopic: String

    fun sendMessage(key: String, message: String): CompletableFuture<SendResult<String, String>> {
        logger.info("메시지 전송: {} - {}", key, message)
        return kafkaTemplate.send(inputTopic, key, message)
            .whenComplete { result, ex ->
                if (ex == null) {
                    logger.info("전송 성공: {} - {} [partition: {}, offset: {}]",
                        key, message, result.recordMetadata.partition(), result.recordMetadata.offset())
                } else {
                    logger.error("전송 실패: {} - {}", key, message, ex)
                }
            }
    }
}