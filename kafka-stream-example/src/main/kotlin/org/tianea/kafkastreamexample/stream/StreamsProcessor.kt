package org.tianea.kafkastreamexample.stream

import org.apache.kafka.streams.StreamsBuilder
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams

@Configuration
@EnableKafkaStreams
class StreamsProcessor {

    private val logger = LoggerFactory.getLogger(StreamsProcessor::class.java)

    @Value("\${kafka.input-topic:input-topic}")
    private lateinit var inputTopic: String

    @Value("\${kafka.output-topic:output-topic}")
    private lateinit var outputTopic: String

    @Bean
    fun kStream(streamsBuilder: StreamsBuilder): Unit {
        // Stream 토폴로지 구성
        streamsBuilder.stream<String, String>(inputTopic)
            .peek { key, value -> logger.info("스트림 입력: {} - {}", key, value) }
            .mapValues { value -> "$value (processed)" }
            .peek { key, value -> logger.info("스트림 출력: {} - {}", key, value) }
            .to(outputTopic)

        logger.info("Kafka Streams 토폴로지가 구성되었습니다.")
    }
}