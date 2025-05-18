package org.tianea.kafkastreamexample.config

import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.*
import java.util.*

@Configuration
class KafkaConfig {

    @Value("\${kafka.bootstrap-servers:localhost:9092}")
    private lateinit var bootstrapServers: String

    @Value("\${kafka.input-topic:input-topic}")
    private lateinit var inputTopic: String

    @Value("\${kafka.output-topic:output-topic}")
    private lateinit var outputTopic: String

    // Spring에서 자동으로 토픽 생성
    @Bean
    fun inputTopic(): NewTopic {
        return TopicBuilder.name(inputTopic)
            .partitions(1)
            .replicas(1)
            .configs(mapOf(
                "min.insync.replicas" to "1"
            ))
            .build()
    }

    @Bean
    fun outputTopic(): NewTopic {
        return TopicBuilder.name(outputTopic)
            .partitions(1)
            .replicas(1)
            .configs(mapOf(
                "min.insync.replicas" to "1"
            ))
            .build()
    }

    // KafkaAdmin 설정
    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val configs = mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            "transaction.state.log.replication.factor" to "1",
            "transaction.state.log.min.isr" to "1",
            "offsets.topic.replication.factor" to "1"
        )
        return KafkaAdmin(configs)
    }

    // Producer 설정
    @Bean
    fun producerConfigs(): Map<String, Any> {
        return mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all"
        )
    }

    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        return DefaultKafkaProducerFactory(producerConfigs())
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }

    // Consumer 설정
    @Bean
    fun consumerConfigs(): Map<String, Any> {
        return mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "kafka-stream-example",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        return DefaultKafkaConsumerFactory(consumerConfigs())
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory()
        return factory
    }

    // Kafka Streams 설정
    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig(): KafkaStreamsConfiguration {
        val props = mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG to "kafka-streams-example",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
            StreamsConfig.COMMIT_INTERVAL_MS_CONFIG to "100",
            StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to "0",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.AT_LEAST_ONCE,
            StreamsConfig.REPLICATION_FACTOR_CONFIG to 1,
            StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG) to 60000
        )
        return KafkaStreamsConfiguration(props)
    }
}