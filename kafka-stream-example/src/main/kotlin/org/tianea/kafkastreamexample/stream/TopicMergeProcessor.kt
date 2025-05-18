package org.tianea.kafkastreamexample.stream

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.common.serialization.Serdes
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams

/**
 * Kafka Streams 프로세서 구성 클래스.
 * 두 개의 입력 토픽을 받아 하나의 출력 토픽으로 병합하는 기능을 구현합니다.
 */
@Configuration
@EnableKafkaStreams
@org.springframework.context.annotation.DependsOn("kafkaTopicConfig")
class TopicMergeProcessor {

    private val logger = LoggerFactory.getLogger(TopicMergeProcessor::class.java)

    @Value("\${kafka.merge-input-topic-1:merge-input-topic-1}")
    private lateinit var mergeInputTopic1: String

    @Value("\${kafka.merge-input-topic-2:merge-input-topic-2}")
    private lateinit var mergeInputTopic2: String

    @Value("\${kafka.merge-output-topic-1:merge-output-topic-1}")
    private lateinit var mergeOutputTopic1: String

    /**
     * Kafka Streams 토폴로지를 구성합니다.
     * 두 입력 토픽(merge-input-topic-1, merge-input-topic-2)의 데이터를 
     * 하나의 출력 토픽(merge-output-topic-1)으로 병합합니다.
     */
    @Bean
    fun topicMergeStream(streamsBuilder: StreamsBuilder): KStream<String, String> {
        val stringSerde = Serdes.String()
        
        // 첫 번째 입력 토픽에서 스트림 생성
        val inputStream1: KStream<String, String> = streamsBuilder.stream(
            mergeInputTopic1,
            Consumed.with(stringSerde, stringSerde)
        )
        
        // 두 번째 입력 토픽에서 스트림 생성
        val inputStream2: KStream<String, String> = streamsBuilder.stream(
            mergeInputTopic2,
            Consumed.with(stringSerde, stringSerde)
        )
        
        // 첫 번째 스트림에 원본 소스 정보 추가 (긴 처리 시간)
        val taggedStream1: KStream<String, String> = inputStream1
            .peek { key, value -> 
                logger.info("입력 토픽1 처리 시작 ({}): key={}, value={}", mergeInputTopic1, key, value)
                try {
                    // 토픽1 메시지 처리에 긴 지연 추가 (500ms)
                    Thread.sleep(500)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
                logger.info("입력 토픽1 처리 완료 ({}): key={}, value={}", mergeInputTopic1, key, value)
            }
            .mapValues { value -> "{ \"source\": \"${mergeInputTopic1}\", \"data\": \"$value\" }" }
        
        // 두 번째 스트림에 원본 소스 정보 추가 (짧은 처리 시간)
        val taggedStream2: KStream<String, String> = inputStream2
            .peek { key, value -> 
                logger.info("입력 토픽2 처리 시작 ({}): key={}, value={}", mergeInputTopic2, key, value)
                try {
                    // 토픽2 메시지 처리에 짧은 지연 추가 (50ms)
                    Thread.sleep(50)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
                logger.info("입력 토픽2 처리 완료 ({}): key={}, value={}", mergeInputTopic2, key, value)
            }
            .mapValues { value -> "{ \"source\": \"${mergeInputTopic2}\", \"data\": \"$value\" }" }
        
        // 두 스트림 병합
        val mergedStream: KStream<String, String> = taggedStream1
            .merge(taggedStream2, Named.`as`("topic-merge-stream"))
            .peek { key, value -> logger.info("병합된 출력: key={}, value={}", key, value) }
        
        // 병합된 스트림을 출력 토픽으로 전송
        mergedStream.to(mergeOutputTopic1, Produced.with(stringSerde, stringSerde))
        
        logger.info("토픽 병합 토폴로지가 구성되었습니다: {} + {} -> {}", 
            mergeInputTopic1, mergeInputTopic2, mergeOutputTopic1)
        
        return mergedStream
    }
}