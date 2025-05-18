# Kafka Streams 예제 프로젝트

이 프로젝트는 Kafka Streams API를 활용한 스트림 처리 기능을 구현한 예제입니다. 특히 스트림 병합(`merge()`)과 조인(`join()`)의 차이점과 동작 방식을 실습을 통해 이해할 수 있도록 구성되어 있습니다.

## 목차

1. [프로젝트 개요](#프로젝트-개요)
2. [기능 구현](#기능-구현)
3. [Kafka Streams 연산 비교](#kafka-streams-연산-비교)
   - [Merge 연산](#merge-연산)
   - [Join 연산](#join-연산)
4. [파티션과 병렬 처리](#파티션과-병렬-처리)
5. [시간 기반 처리](#시간-기반-처리)
6. [API 엔드포인트](#api-엔드포인트)
7. [실행 방법](#실행-방법)
8. [주요 학습 포인트](#주요-학습-포인트)

## 프로젝트 개요

이 프로젝트는 Spring Boot와 Kafka Streams를 이용하여 실시간 스트림 처리 애플리케이션을 구현합니다. 주요 기능으로 다음과 같은 항목을 다룹니다:

- 단일 토픽 스트림 처리 (기본 예제)
- 다중 토픽 병합 (Merge 연산)
- 키 기반 스트림 조인 (Join 연산)
- 시간 윈도우 처리 (Windowed Join)
- 파티션 기반 병렬 처리

## 기능 구현

### 1. 기본 스트림 처리 (`StreamsProcessor`)

단일 입력 토픽에서 메시지를 읽어 처리한 후 출력 토픽으로 전송하는 기본적인 스트림 처리 로직을 구현합니다.

```kotlin
@Bean
fun kStream(streamsBuilder: StreamsBuilder): KStream<String, String> {
    val stream = streamsBuilder.stream<String, String>(inputTopic)
    
    stream
        .peek { key, value -> logger.info("스트림 입력: {} - {}", key, value) }
        .mapValues { value -> "$value (processed)" }
        .peek { key, value -> logger.info("스트림 출력: {} - {}", key, value) }
        .to(outputTopic)
    
    return stream
}
```

### 2. 토픽 병합 (`TopicMergeProcessor`)

두 개의 입력 토픽에서 메시지를 읽어 하나의 출력 토픽으로 병합합니다. 각 메시지의 처리 시간에 차이를 두어 병합 동작을 관찰할 수 있습니다.

```kotlin
@Bean
fun topicMergeStream(streamsBuilder: StreamsBuilder): KStream<String, String> {
    // 첫 번째 스트림 (처리 시간 500ms)
    val taggedStream1: KStream<String, String> = inputStream1
        .peek { key, value -> 
            logger.info("입력 토픽1 처리 시작: key={}, value={}", key, value)
            Thread.sleep(500)  // 긴 처리 시간
            logger.info("입력 토픽1 처리 완료: key={}, value={}", key, value)
        }
        .mapValues { value -> "{ \"source\": \"topic1\", \"data\": \"$value\" }" }
    
    // 두 번째 스트림 (처리 시간 50ms)
    val taggedStream2: KStream<String, String> = inputStream2
        .peek { key, value -> 
            logger.info("입력 토픽2 처리 시작: key={}, value={}", key, value)
            Thread.sleep(50)  // 짧은 처리 시간
            logger.info("입력 토픽2 처리 완료: key={}, value={}", key, value)
        }
        .mapValues { value -> "{ \"source\": \"topic2\", \"data\": \"$value\" }" }
    
    // 두 스트림 병합
    val mergedStream: KStream<String, String> = taggedStream1
        .merge(taggedStream2)
        .peek { key, value -> logger.info("병합된 출력: key={}, value={}", key, value) }
    
    mergedStream.to(mergeOutputTopic1)
    
    return mergedStream
}
```

### 3. 스트림 조인 (`JoinStreamsProcessor`)

키를 기준으로 두 토픽의 메시지를 조인합니다. 시간 윈도우 내에서만 조인이 수행되며, 동일한 키를 가진 레코드만 결합됩니다.

```kotlin
@Bean
fun joinStream(streamsBuilder: StreamsBuilder): KStream<String, String> {
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
    
    joinedStream.to(joinOutputTopic)
    
    return joinedStream
}
```

## Kafka Streams 연산 비교

### Merge 연산

`merge()` 연산은 두 스트림의 모든 메시지를 하나의 스트림으로 단순 결합합니다.

**특징:**
- 키나 값에 관계없이 모든 메시지를 하나의 스트림으로 병합
- 메시지 처리 시간에 따라 출력 순서가 결정됨
- 동일한 키가 두 스트림에 있어도 별도로 처리됨
- 시간적 제약 없음 (도착 순서대로 처리)

**사용 사례:**
- 여러 소스의 데이터를 단순히 하나의 스트림으로 모으는 경우
- 메시지 간 관계가 중요하지 않은 경우
- 단순 로깅, 모니터링 등의 용도

### Join 연산

`join()` 연산은 동일한 키를 가진 두 스트림의 메시지를 조인하여 새로운 메시지를 생성합니다.

**특징:**
- 동일한 키를 가진 메시지만 조인됨
- 설정된 시간 윈도우 내에서만 조인 수행 (예: 5초 이내)
- 두 스트림의 값이 결합된 새로운 메시지 생성
- 조인 결과는 양쪽 스트림에 모두 해당 키의 메시지가 있을 때만 발생

**사용 사례:**
- 관련 이벤트를 결합해야 하는 경우 (주문과 결제 정보 등)
- 보완적인 데이터를 결합하여 풍부한 정보를 생성해야 하는 경우
- 이벤트 상관관계 분석 및 복합 이벤트 처리

## 파티션과 병렬 처리

Kafka는 토픽을 파티션으로 분할하여 병렬 처리를 지원합니다. 이 프로젝트에서는 파티션 수와 컨슈머 동시성을 조정하여 병렬 처리의 효과를 확인할 수 있습니다.

**파티션 설정:**
```kotlin
@Bean
fun joinInputTopic1(): NewTopic {
    return TopicBuilder.name(joinInputTopic1)
        .partitions(3)  // 파티션 수를 3으로 설정
        .replicas(1)
        .build()
}
```

**컨슈머 동시성 설정:**
```kotlin
@KafkaListener(
    topics = ["\${kafka.join-output-topic:join-output-topic}"],
    groupId = "join-consumer-group",
    concurrency = "3"  // 각 리스너마다 3개의 스레드 사용
)
```

**Kafka Streams 스레드 설정:**
```yaml
spring:
  kafka:
    streams:
      properties:
        num.stream.threads: 3  # Kafka Streams 처리 스레드 수
```

**주요 학습 포인트:**
- 파티션 수를 늘리면 병렬 처리 가능성이 증가하지만, 컨슈머 동시성도 함께 높여야 효과적
- 메시지의 키는 파티션 할당에 영향을 미침 (동일한 키는 항상 동일한 파티션으로 전송)
- 파티션 간 순서는 보장되지 않지만, 파티션 내 순서는 보장됨

## 시간 기반 처리

Kafka Streams는 시간 윈도우 기반 처리를 지원합니다. `JoinStreamsProcessor`에서는 시간 윈도우를 이용한 조인 연산을 구현하여 다양한 시나리오를 테스트할 수 있습니다.

**시간 윈도우 설정:**
```kotlin
// 5초 시간 윈도우 내에서 조인
val joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5))
```

**테스트 시나리오:**
- 윈도우 내 조인: 두 메시지가 5초 이내에 도착하면 조인됨
- 윈도우 밖 조인: 두 메시지가 5초를 초과하면 조인되지 않음
- 다른 키: 키가 다른 메시지는 조인되지 않음

## API 엔드포인트

### 기본 메시지 발행

- `POST /api/kafka/publish`: 기본 입력 토픽에 메시지 발행
- `GET /api/kafka/test`: 테스트 메시지 5개 발행

### Merge 테스트

- `POST /api/merge/topic1`: 첫 번째 병합 토픽에 메시지 발행
- `POST /api/merge/topic2`: 두 번째 병합 토픽에 메시지 발행
- `GET /api/merge/test`: 두 토픽에 각각 3개의 테스트 메시지 발행
- `GET /api/merge/test-concurrent`: 두 토픽에 많은 메시지를 비동기적으로 발행

### Join 테스트

- `POST /api/join/topic1`: 첫 번째 조인 토픽에 메시지 발행
- `POST /api/join/topic2`: 두 번째 조인 토픽에 메시지 발행
- `GET /api/join/test`: 두 토픽에 동일한 키로 메시지 발행
- `GET /api/join/test-windowed`: 시간 간격을 두고 메시지 발행 (윈도우 테스트)
- `GET /api/join/test-random`: 랜덤 키와 시간으로 메시지 발행 (분산 환경 시뮬레이션)

## 실행 방법

1. Kafka 클러스터 실행 (Docker Compose 사용)
   ```bash
   docker-compose up -d
   ```

2. 애플리케이션 빌드 및 실행
   ```bash
   ./gradlew bootRun
   ```

3. API 테스트
   ```bash
   # 기본 테스트
   curl http://localhost:8081/api/kafka/test
   
   # Merge 테스트
   curl http://localhost:8081/api/merge/test-concurrent
   
   # Join 테스트
   curl http://localhost:8081/api/join/test-windowed
   ```

## 주요 학습 포인트

1. **메시지 처리 순서**
   - Kafka Streams에서 `merge()` 연산은 처리 속도에 따라 출력 순서가 결정됩니다.
   - 단일 파티션/스레드 환경에서는 순서가 유지될 수 있지만, 멀티 파티션 환경에서는 순서가 보장되지 않습니다.

2. **키 기반 처리**
   - 동일한 키를 가진 메시지는 항상 동일한 파티션에 할당됩니다.
   - `join()` 연산은 동일한 키를 가진 메시지만 조인합니다.

3. **파티션과 병렬성**
   - 파티션 수를 증가시키면 병렬 처리가 가능하지만, 컨슈머 동시성도 함께 증가시켜야 효과적입니다.
   - 파티션 내에서 메시지 순서는 보장되지만 파티션 간 메시지 순서는 보장되지 않습니다.

4. **시간 기반 처리**
   - Kafka Streams는 이벤트 시간 기반 처리를 지원합니다.
   - 윈도우 조인은 설정된 시간 내에 도착한 메시지만 조인합니다.

5. **스테이트풀 처리**
   - Kafka Streams는 상태 저장 처리를 지원하며, 이를 통해 복잡한 스트림 처리 작업을 구현할 수 있습니다.
   - Merge는 스테이트리스 연산이지만, Join은 스테이트풀 연산입니다.

---

이 프로젝트를 통해 Kafka Streams API의 다양한 연산 방식과 병렬 처리 모델을 이해하고, 실시간 스트림 처리 애플리케이션을 개발하는 방법을 학습할 수 있습니다.