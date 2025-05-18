package org.tianea.kafkastreamexample.controller

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import org.tianea.kafkastreamexample.model.MessageRequest
import org.tianea.kafkastreamexample.service.JoinProducerService
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Join 기능을 테스트하기 위한 컨트롤러
 */
@RestController
@RequestMapping("/api/join")
class JoinController(
    private val joinProducerService: JoinProducerService
) {
    private val logger = LoggerFactory.getLogger(JoinController::class.java)

    /**
     * 첫 번째 Join 토픽으로 메시지를 발행합니다.
     */
    @PostMapping("/topic1")
    fun publishToTopic1(@RequestBody request: MessageRequest): String {
        logger.info("Join 토픽1 메시지 발행 요청: {}", request)
        joinProducerService.sendToTopic1(request.key, request.message)
        return "메시지가 성공적으로 Join 토픽1에 발행되었습니다."
    }

    /**
     * 두 번째 Join 토픽으로 메시지를 발행합니다.
     */
    @PostMapping("/topic2")
    fun publishToTopic2(@RequestBody request: MessageRequest): String {
        logger.info("Join 토픽2 메시지 발행 요청: {}", request)
        joinProducerService.sendToTopic2(request.key, request.message)
        return "메시지가 성공적으로 Join 토픽2에 발행되었습니다."
    }

    /**
     * 동일한 키를 사용하여 두 Join 토픽에 동시에 메시지를 발행합니다.
     * 이를 통해 Join 동작을 테스트할 수 있습니다.
     */
    @GetMapping("/test")
    fun testJoin(): Map<String, String> {
        logger.info("Join 테스트 메시지 발행 시작")
        
        val results = mutableMapOf<String, String>()
        
        // 두 토픽에 동일한 키로 메시지 발행
        for (i in 1..3) {
            val key = "join-key-$i"
            val message1 = "Join 토픽1 테스트 메시지 $i"
            val message2 = "Join 토픽2 테스트 메시지 $i"
            
            joinProducerService.sendToTopic1(key, message1)
            joinProducerService.sendToTopic2(key, message2)
        }
        
        results["result"] = "6개의 테스트 메시지가 Join 토픽에 발행되었습니다."
        return results
    }
    
    /**
     * 두 Join 토픽에 동일한 키로 시간 간격을 두고 메시지를 발행합니다.
     * 이를 통해 시간 윈도우 Join 동작을 테스트할 수 있습니다.
     */
    @GetMapping("/test-windowed")
    fun testWindowedJoin(): Map<String, String> {
        logger.info("Join 윈도우 테스트 메시지 발행 시작")
        
        val results = mutableMapOf<String, String>()
        val executor = Executors.newScheduledThreadPool(2)
        
        // 동일한 키를 사용하지만 시간차를 두고 발행
        for (i in 1..3) {
            val key = "windowed-key-$i"
            val message1 = "Join 토픽1 윈도우 테스트 메시지 $i"
            val message2 = "Join 토픽2 윈도우 테스트 메시지 $i"
            
            // 토픽1에 즉시 발행
            joinProducerService.sendToTopic1(key, message1)
            logger.info("토픽1에 윈도우 테스트 메시지 발행: {} - {}", key, message1)
            
            // 토픽2에는 2초 후 발행 (5초 윈도우 내에서 Join 됨)
            executor.schedule({
                joinProducerService.sendToTopic2(key, message2)
                logger.info("토픽2에 윈도우 테스트 메시지 발행 (2초 지연): {} - {}", key, message2)
            }, 2, TimeUnit.SECONDS)
        }
        
        // 윈도우 범위를 벗어나는 케이스 (6초 후 발행)
        val lateKey = "late-key"
        val lateMessage1 = "Join 토픽1 지연 테스트 메시지"
        val lateMessage2 = "Join 토픽2 지연 테스트 메시지"
        
        joinProducerService.sendToTopic1(lateKey, lateMessage1)
        logger.info("토픽1에 지연 메시지 발행: {} - {}", lateKey, lateMessage1)
        
        // 6초 후 발행 (5초 윈도우를 벗어남)
        executor.schedule({
            joinProducerService.sendToTopic2(lateKey, lateMessage2)
            logger.info("토픽2에 지연 메시지 발행 (6초 지연, 윈도우 초과): {} - {}", lateKey, lateMessage2)
        }, 6, TimeUnit.SECONDS)
        
        // 서로 다른 키 테스트 (Join 되지 않음)
        val diffKey1 = "different-key-1"
        val diffKey2 = "different-key-2"
        
        joinProducerService.sendToTopic1(diffKey1, "Join 토픽1 다른 키 테스트")
        joinProducerService.sendToTopic2(diffKey2, "Join 토픽2 다른 키 테스트")
        
        // 실행기 종료 (비동기 작업은 계속 진행됨)
        executor.shutdown()
        
        results["result"] = "Join 윈도우 테스트가 시작되었습니다. 약 6초간 메시지가 순차적으로 발행됩니다."
        return results
    }
    
    /**
     * 두 Join 토픽에 랜덤하게 메시지를 발행합니다.
     * 이를 통해 분산 환경에서의 Join 동작을 테스트할 수 있습니다.
     */
    @GetMapping("/test-random")
    fun testRandomJoin(): Map<String, String> {
        logger.info("Join 랜덤 테스트 메시지 발행 시작")
        
        val results = mutableMapOf<String, String>()
        val messageCount = 20
        val executor = Executors.newFixedThreadPool(10)
        val futures = mutableListOf<CompletableFuture<Void>>()
        val random = Random()
        
        // 공통 키 준비
        val commonKeys = (1..10).map { "common-key-$it" }
        
        // 랜덤하게 메시지 발행
        for (i in 1..messageCount) {
            // 1/3 확률로 동일한 키, 2/3 확률로 다른 키 사용
            val useCommonKey = random.nextInt(3) == 0
            val key = if (useCommonKey) {
                commonKeys[random.nextInt(commonKeys.size)]
            } else {
                "random-key-${UUID.randomUUID().toString().substring(0, 6)}"
            }
            
            // 랜덤하게 토픽1 또는 토픽2에 메시지 발행
            val sendToTopic1 = random.nextBoolean()
            
            futures.add(CompletableFuture.runAsync({
                if (sendToTopic1) {
                    val message = "Join 토픽1 랜덤 테스트 메시지 $i"
                    joinProducerService.sendToTopic1(key, message)
                    logger.info("토픽1에 랜덤 메시지 발행: {} - {}", key, message)
                } else {
                    val message = "Join 토픽2 랜덤 테스트 메시지 $i"
                    joinProducerService.sendToTopic2(key, message)
                    logger.info("토픽2에 랜덤 메시지 발행: {} - {}", key, message)
                }
                
                // 0-500ms 지연 추가
                Thread.sleep(random.nextInt(500).toLong())
            }, executor))
        }
        
        // 모든 CompletableFuture가 완료될 때까지 기다림
        CompletableFuture.allOf(*futures.toTypedArray()).join()
        
        // 실행기 종료
        executor.shutdown()
        
        results["result"] = "총 ${messageCount}개의 테스트 메시지가 랜덤하게 발행되었습니다."
        return results
    }
}