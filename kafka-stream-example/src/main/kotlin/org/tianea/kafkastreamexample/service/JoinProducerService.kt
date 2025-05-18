package org.tianea.kafkastreamexample.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

/**
 * Join 테스트를 위한 메시지 전송 서비스
 */
@Service
class JoinProducerService(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(JoinProducerService::class.java)

    @Value("\${kafka.join-input-topic-1:join-input-topic-1}")
    private lateinit var joinInputTopic1: String

    @Value("\${kafka.join-input-topic-2:join-input-topic-2}")
    private lateinit var joinInputTopic2: String

    /**
     * 첫 번째 입력 토픽으로 메시지를 전송합니다.
     */
    fun sendToTopic1(key: String, message: String): CompletableFuture<SendResult<String, String>> {
        logger.info("메시지 전송 (Join 토픽1): {} - {}", key, message)
        return kafkaTemplate.send(joinInputTopic1, key, message)
            .whenComplete { result, ex ->
                if (ex == null) {
                    logger.info(
                        "전송 성공 (Join 토픽1): {} - {} [partition: {}, offset: {}]",
                        key, message, result.recordMetadata.partition(), result.recordMetadata.offset()
                    )
                } else {
                    logger.error("전송 실패 (Join 토픽1): {} - {}", key, message, ex)
                }
            }
    }

    /**
     * 두 번째 입력 토픽으로 메시지를 전송합니다.
     */
    fun sendToTopic2(key: String, message: String): CompletableFuture<SendResult<String, String>> {
        logger.info("메시지 전송 (Join 토픽2): {} - {}", key, message)
        return kafkaTemplate.send(joinInputTopic2, key, message)
            .whenComplete { result, ex ->
                if (ex == null) {
                    logger.info(
                        "전송 성공 (Join 토픽2): {} - {} [partition: {}, offset: {}]",
                        key, message, result.recordMetadata.partition(), result.recordMetadata.offset()
                    )
                } else {
                    logger.error("전송 실패 (Join 토픽2): {} - {}", key, message, ex)
                }
            }
    }
}