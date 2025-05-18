package org.tianea.kafkastreamexample.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler

/**
 * Kafka 컨슈머 설정 클래스
 * 리스너 컨테이너 팩토리 설정을 통해 컨슈머의 동시성을 제어합니다.
 */
@Configuration
class KafkaConsumerConfig(
    private val consumerFactory: ConsumerFactory<String, String>,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(KafkaConsumerConfig::class.java)

    /**
     * 카프카 리스너 컨테이너 팩토리를 설정합니다.
     * 동시성과 파티션 할당 전략을 세부적으로 제어합니다.
     */
    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
            .apply {
                consumerFactory = consumerFactory
                setConcurrency(3)
                isBatchListener = false
                setAutoStartup(true)
                containerProperties.ackMode = ContainerProperties.AckMode.RECORD
                containerProperties.isMissingTopicsFatal = false
                setCommonErrorHandler(DefaultErrorHandler { record, exception ->
                    logger.error("메시지 처리 중 오류 발생: ${record.key()} - ${record.value()}", exception)
                })
            }

        return factory
    }
}