package org.tianea.kafkastreamexample

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
class KafkaStreamExampleApplication

fun main(args: Array<String>) {
    runApplication<KafkaStreamExampleApplication>(*args)
}
