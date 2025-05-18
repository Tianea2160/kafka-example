package org.tianea.kafkastreamexample.service

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import java.lang.Thread.sleep

@Service
class KafkaConsumerService {

    private val logger = LoggerFactory.getLogger(KafkaConsumerService::class.java)

    @KafkaListener(
        topics = ["\${kafka.output-topic:output-topic}", "\${kafka.input-topic:input-topic}"],
        groupId = "output-consumer-group",
        concurrency = "3"  // 동시성 설정 추가
    )
    fun listen(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_KEY) key: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        logger.info("처리된 메시지 수신: $key - $message [topic : $topic, partition: $partition, offset: $offset]")
    }

    @KafkaListener(
        topics = [
            "\${kafka.merge-input-topic-1:merge-input-topic-1}",
        ],
        groupId = "output-consumer-group",
        concurrency = "3"  // 동시성 설정 추가
    )
    fun listenMergeStream1(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_KEY) key: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        logger.info("처리된 메시지 수신: $key - $message [topic : $topic, partition: $partition, offset: $offset]")
        sleep(500)
    }

    @KafkaListener(
        topics = [
            "\${kafka.merge-input-topic-2:merge-input-topic-2}",
        ],
        groupId = "output-consumer-group",
        concurrency = "3"  // 동시성 설정 추가
    )
    fun listenMergeStream2(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_KEY) key: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        logger.info("처리된 메시지 수신: $key - $message [topic : $topic, partition: $partition, offset: $offset]")
    }
    
    @KafkaListener(
        topics = ["\${kafka.join-output-topic:join-output-topic}"],
        groupId = "join-consumer-group",
        concurrency = "3"
    )
    fun listenJoinResults(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_KEY) key: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        logger.info("JOIN 결과 메시지 수신: $key - $message [partition: $partition, offset: $offset]")
    }
}