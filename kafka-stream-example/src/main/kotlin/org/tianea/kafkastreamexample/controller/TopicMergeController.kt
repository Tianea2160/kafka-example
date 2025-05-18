package org.tianea.kafkastreamexample.controller

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import org.tianea.kafkastreamexample.model.MessageRequest
import org.tianea.kafkastreamexample.service.TopicMergeProducerService
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

/**
 * 토픽 병합 기능을 위한 API 컨트롤러
 */
@RestController
@RequestMapping("/api/merge")
class TopicMergeController(
    private val topicMergeProducerService: TopicMergeProducerService
) {
    private val logger = LoggerFactory.getLogger(TopicMergeController::class.java)

    /**
     * 첫 번째 입력 토픽으로 메시지를 발행합니다.
     */
    @PostMapping("/topic1")
    fun publishToTopic1(@RequestBody request: MessageRequest): String {
        logger.info("토픽1 메시지 발행 요청: {}", request)
        topicMergeProducerService.sendToTopic1(request.key, request.message)
        return "메시지가 성공적으로 토픽1에 발행되었습니다."
    }

    /**
     * 두 번째 입력 토픽으로 메시지를 발행합니다.
     */
    @PostMapping("/topic2")
    fun publishToTopic2(@RequestBody request: MessageRequest): String {
        logger.info("토픽2 메시지 발행 요청: {}", request)
        topicMergeProducerService.sendToTopic2(request.key, request.message)
        return "메시지가 성공적으로 토픽2에 발행되었습니다."
    }

    /**
     * 두 토픽 모두에 테스트 메시지를 발행합니다.
     */
    @GetMapping("/test")
    fun testMerge(): Map<String, String> {
        logger.info("병합 테스트 메시지 발행 시작")

        val results = mutableMapOf<String, String>()

        // 토픽1에 테스트 메시지 발행
        for (i in 1..3) {
            val key = "topic1-key-$i"
            val message = "Topic1 테스트 메시지 $i"
            topicMergeProducerService.sendToTopic1(key, message)
        }
        results["topic1"] = "3개의 테스트 메시지가 토픽1에 발행되었습니다."

        // 토픽2에 테스트 메시지 발행
        for (i in 1..3) {
            val key = "topic2-key-$i"
            val message = "Topic2 테스트 메시지 $i"
            topicMergeProducerService.sendToTopic2(key, message)
        }
        results["topic2"] = "3개의 테스트 메시지가 토픽2에 발행되었습니다."

        results["result"] = "총 6개의 테스트 메시지가 병합 처리를 위해 발행되었습니다."
        return results
    }

    /**
     * 두 토픽에 동시에 많은 메시지를 비동기적으로 발행합니다.
     * 이 테스트는 병합 동작을 더 현실적으로 보여줍니다.
     */
    @GetMapping("/test-concurrent")
    fun testConcurrentMerge(): Map<String, String> {
        logger.info("병합 동시 테스트 메시지 발행 시작")

        val results = mutableMapOf<String, String>()
        val messageCount = 30 // 각 토픽당 메시지 수 증가 (10 -> 30)

        // 실행기 생성
        val executor = Executors.newFixedThreadPool(20) // 스레드 풀 크기 증가 (10 -> 20)
        val futures = mutableListOf<CompletableFuture<Void>>()

        // 토픽1과 토픽2에 동시에 메시지 발행
        for (i in 1..messageCount) {
            // 토픽1에 메시지 발행 (각 메시지마다 다른 스레드)
            futures.add(CompletableFuture.runAsync({
                val key = "concurrent-key-$i-${UUID.randomUUID().toString().substring(0, 6)}"
                val message = "Topic1 동시성 테스트 메시지 $i"
                logger.info("토픽1에 비동기 메시지 발행: {} - {}", key, message)
                topicMergeProducerService.sendToTopic1(key, message)
            }, executor))

            // 토픽2에 메시지 발행 (각 메시지마다 다른 스레드)
            futures.add(CompletableFuture.runAsync({
                val key = "concurrent-key-$i-${UUID.randomUUID().toString().substring(0, 6)}"
                val message = "Topic2 동시성 테스트 메시지 $i"
                logger.info("토픽2에 비동기 메시지 발행: {} - {}", key, message)
                topicMergeProducerService.sendToTopic2(key, message)
            }, executor))
        }

        // 동일한 키로 메시지 몇 개 발행 (동기화 테스트용)
        for (i in 1..3) {
            val sharedKey = "shared-key-$i"

            futures.add(CompletableFuture.runAsync({
                val message = "Topic1 동일 키 메시지 $i"
                logger.info("토픽1에 동일 키 메시지 발행: {} - {}", sharedKey, message)
                topicMergeProducerService.sendToTopic1(sharedKey, message)
            }, executor))

            futures.add(CompletableFuture.runAsync({
                val message = "Topic2 동일 키 메시지 $i"
                logger.info("토픽2에 동일 키 메시지 발행: {} - {}", sharedKey, message)
                topicMergeProducerService.sendToTopic2(sharedKey, message)
            }, executor))
        }

        // 모든 CompletableFuture가 완료될 때까지 기다림
        CompletableFuture.allOf(*futures.toTypedArray()).join()

        // 실행기 종료
        executor.shutdown()

        val totalMessages = messageCount * 2 + 6 // 랜덤 키 메시지 + 동일 키 메시지
        results["result"] = "총 ${totalMessages}개의 테스트 메시지가 동시에 발행되었습니다."
        return results
    }
}