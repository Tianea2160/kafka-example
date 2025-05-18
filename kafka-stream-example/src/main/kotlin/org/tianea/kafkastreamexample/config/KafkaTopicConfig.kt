package org.tianea.kafkastreamexample.config

import jakarta.annotation.PostConstruct
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin

@Configuration
class KafkaTopicConfig(
    private val kafkaAdmin: KafkaAdmin
) {
    private val logger = LoggerFactory.getLogger(KafkaTopicConfig::class.java)

    @Value("\${kafka.input-topic:input-topic}")
    private lateinit var inputTopic: String

    @Value("\${kafka.input-topic2:input-topic2}")
    private lateinit var inputTopic2: String

    @Value("\${kafka.output-topic:output-topic}")
    private lateinit var outputTopic: String

    @Value("\${kafka.merged-topic:merged-topic}")
    private lateinit var mergedTopic: String

    @Value("\${kafka.merge-input-topic-1:merge-input-topic-1}")
    private lateinit var mergeInputTopic1: String

    @Value("\${kafka.merge-input-topic-2:merge-input-topic-2}")
    private lateinit var mergeInputTopic2: String

    @Value("\${kafka.merge-output-topic-1:merge-output-topic-1}")
    private lateinit var mergeOutputTopic1: String

    @Value("\${kafka.join-input-topic-1:join-input-topic-1}")
    private lateinit var joinInputTopic1: String

    @Value("\${kafka.join-input-topic-2:join-input-topic-2}")
    private lateinit var joinInputTopic2: String

    @Value("\${kafka.join-output-topic:join-output-topic}")
    private lateinit var joinOutputTopic: String

    @PostConstruct
    fun init() {
        // AdminClient를 사용하여 토픽이 존재하는지 확인하고 필요시 생성
        try {
            val adminClient = AdminClient.create(kafkaAdmin.configurationProperties)
            val topicNames = adminClient.listTopics().names().get()

            val topicsToCreate = mutableListOf<NewTopic>()

            // 필요한 토픽들이 존재하는지 확인하고 없으면 생성 목록에 추가
            for (topic in listOf(
                inputTopic, inputTopic2, outputTopic, mergedTopic,
                mergeInputTopic1, mergeInputTopic2, mergeOutputTopic1,
                joinInputTopic1, joinInputTopic2, joinOutputTopic
            )) {
                if (!topicNames.contains(topic)) {
                    topicsToCreate.add(
                        TopicBuilder.name(topic)
                            .partitions(if (topic == mergeInputTopic1 || topic == mergeInputTopic2 || topic == mergeOutputTopic1 ||
                                           topic == joinInputTopic1 || topic == joinInputTopic2 || topic == joinOutputTopic) 3 else 1)
                            .replicas(1)
                            .build()
                    )
                    logger.info("토픽 생성 예정: {}", topic)
                } else {
                    logger.info("토픽 이미 존재함: {}", topic)
                }
            }

            // 필요한 토픽들이 있다면 생성
            if (topicsToCreate.isNotEmpty()) {
                adminClient.createTopics(topicsToCreate).all().get()
                logger.info("모든 필요 토픽이 생성되었습니다.")
            }

            adminClient.close()
        } catch (e: Exception) {
            logger.error("토픽 초기화 중 오류 발생", e)
        }
    }

    // 필수 토픽을 Bean으로 등록하여 Spring이 관리하도록 함
    @Bean
    fun mergeInputTopic1(): NewTopic {
        return TopicBuilder.name(mergeInputTopic1)
            .partitions(3)  // 파티션 수를 3으로 증가
            .replicas(1)
            .build()
    }

    @Bean
    fun mergeInputTopic2(): NewTopic {
        return TopicBuilder.name(mergeInputTopic2)
            .partitions(3)  // 파티션 수를 3으로 증가
            .replicas(1)
            .build()
    }

    @Bean
    fun mergeOutputTopic1(): NewTopic {
        return TopicBuilder.name(mergeOutputTopic1)
            .partitions(3)  // 파티션 수를 3으로 증가
            .replicas(1)
            .build()
    }
    
    @Bean
    fun joinInputTopic1(): NewTopic {
        return TopicBuilder.name(joinInputTopic1)
            .partitions(3)
            .replicas(1)
            .build()
    }

    @Bean
    fun joinInputTopic2(): NewTopic {
        return TopicBuilder.name(joinInputTopic2)
            .partitions(3)
            .replicas(1)
            .build()
    }

    @Bean
    fun joinOutputTopic(): NewTopic {
        return TopicBuilder.name(joinOutputTopic)
            .partitions(3)
            .replicas(1)
            .build()
    }
}