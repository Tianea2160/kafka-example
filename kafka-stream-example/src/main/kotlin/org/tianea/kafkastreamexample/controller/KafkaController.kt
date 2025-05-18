package org.tianea.kafkastreamexample.controller

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import org.tianea.kafkastreamexample.model.MessageRequest
import org.tianea.kafkastreamexample.service.KafkaProducerService

@RestController
@RequestMapping("/api/kafka")
class KafkaController(
    private val kafkaProducerService: KafkaProducerService
) {
    private val logger = LoggerFactory.getLogger(KafkaController::class.java)

    @PostMapping("/publish")
    fun publish(@RequestBody request: MessageRequest): String {
        logger.info("메시지 발행 요청: {}", request)
        kafkaProducerService.sendMessage(request.key, request.message)
        return "메시지가 성공적으로 발행되었습니다."
    }

    @GetMapping("/test")
    fun test(): String {
        logger.info("테스트 메시지 발행 시작")
        for (i in 1..5) {
            val key = "key-$i"
            val message = "test-message-$i"
            kafkaProducerService.sendMessage(key, message)
        }
        return "5개의 테스트 메시지가 발행되었습니다."
    }
}