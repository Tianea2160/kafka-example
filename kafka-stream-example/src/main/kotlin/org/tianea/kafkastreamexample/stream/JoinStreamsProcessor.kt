package org.tianea.kafkastreamexample.stream

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.common.serialization.Serdes
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import java.time.Duration

/**
 * Kafka Streams Join 프로세서 구성 클래스.
 * 두 개의 입력 토픽을 받아 동일한 키를 기준으로 조인하여 하나의 출력 토픽으로 병합합니다.
 * 이 방식은 단순 병합(merge)과 달리 동일한 키를 가진 메시지를 논리적으로 결합합니다.
 */
@Configuration
@EnableKafkaStreams
@org.springframework.context.annotation.DependsOn("kafkaTopicConfig")
class JoinStreamsProcessor {

    private val logger = LoggerFactory.getLogger(JoinStreamsProcessor::class.java)

    @Value("\${kafka.join-input-topic-1:join-input-topic-1}")
    private lateinit var joinInputTopic1: String

    @Value("\${kafka.join-input-topic-2:join-input-topic-2}")
    private lateinit var joinInputTopic2: String

    @Value("\${kafka.join-output-topic:join-output-topic}")
    private lateinit var joinOutputTopic: String

    /**
     * Kafka Streams 토폴로지를 구성합니다.
     * 두 입력 토픽의 데이터를 동일한 키를 기준으로 조인하여 하나의 출력 토픽으로 전송합니다.
     */
    @Bean
    fun joinStream(streamsBuilder: StreamsBuilder): KStream<String, String> {
        val stringSerde = Serdes.String()
        
        // 첫 번째 입력 토픽에서 스트림 생성
        val stream1: KStream<String, String> = streamsBuilder.stream(
            joinInputTopic1,
            Consumed.with(stringSerde, stringSerde)
        )
        
        // 두 번째 입력 토픽에서 스트림 생성
        val stream2: KStream<String, String> = streamsBuilder.stream(
            joinInputTopic2,
            Consumed.with(stringSerde, stringSerde)
        )
        
        // 로깅 추가
        val loggedStream1 = stream1.peek { key, value -> 
            logger.info("Join 입력 토픽1 수신: key={}, value={}", key, value)
        }
        
        val loggedStream2 = stream2.peek { key, value -> 
            logger.info("Join 입력 토픽2 수신: key={}, value={}", key, value)
        }
        
        // 윈도우 조인 설정 (5초 내에 도착한 메시지 결합)
        val joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5))
        
        // 스트림 조인 수행 (동일한 키를 가진 메시지 결합)
        val joinedStream: KStream<String, String> = loggedStream1.join(
            loggedStream2,
            { value1, value2 -> 
                """{"topic1": "$value1", "topic2": "$value2", "joinTime": "${System.currentTimeMillis()}"}"""
            },
            joinWindow,
            StreamJoined.with(stringSerde, stringSerde, stringSerde)
        )
        
        // 조인 결과 로깅
        val resultStream = joinedStream.peek { key, value -> 
            logger.info("Join 결과: key={}, value={}", key, value)
        }
        
        // 조인된 스트림을 출력 토픽으로 전송
        resultStream.to(joinOutputTopic, Produced.with(stringSerde, stringSerde))
        
        logger.info("스트림 조인 토폴로지가 구성되었습니다: {} + {} -> {}", 
            joinInputTopic1, joinInputTopic2, joinOutputTopic)
        
        return resultStream
    }
}