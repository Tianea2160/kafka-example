package org.tianea.kafkastreamexample.runner

import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import org.tianea.kafkastreamexample.service.KafkaProducerService

/**
 * 애플리케이션 시작 시 자동으로 테스트 메시지를 발행합니다.
 * "test" 프로필이 활성화된 경우에만 실행됩니다.
 */
@Component
@Profile("test")
class ApplicationRunner(
    private val kafkaProducerService: KafkaProducerService
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(ApplicationRunner::class.java)

    override fun run(args: ApplicationArguments) {
        logger.info("애플리케이션 시작 시 테스트 메시지 발행 중...")
        
        for (i in 1..5) {
            val key = "auto-key-$i"
            val message = "auto-message-$i"
            kafkaProducerService.sendMessage(key, message)
        }
        
        logger.info("테스트 메시지 발행 완료")
    }
}