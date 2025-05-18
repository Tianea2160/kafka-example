import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import java.time.Duration
import java.util.*

fun main() {
    val bootstrapServers = "localhost:9092"
    val inputTopic = "input-topic"
    val outputTopic = "output-topic"

    createTopicIfNotExists(bootstrapServers, inputTopic)
    createTopicIfNotExists(bootstrapServers, outputTopic)

    // Kafka Streams 토폴로지 설정
    val builder = StreamsBuilder()
    builder.stream<String, String>(inputTopic)
        .peek { key, value -> println("🔄 Stream received: $key -> $value") }
        .mapValues { value -> "$value (processed)" }
        .to(outputTopic)

    val streamsProps = Properties().apply {
        put("bootstrap.servers", bootstrapServers)
        put("application.id", "stream-app-${System.currentTimeMillis()}")
        put("default.key.serde", "org.apache.kafka.common.serialization.Serdes\$StringSerde")
        put("default.value.serde", "org.apache.kafka.common.serialization.Serdes\$StringSerde")
    }
    val streams = org.apache.kafka.streams.KafkaStreams(builder.build(), streamsProps)
    streams.start()

    // input-topic으로 메시지 전송
    val producerProps = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    }
    KafkaProducer<String, String>(producerProps).use { producer ->
        for (i in 1..5) {
            val record = ProducerRecord(inputTopic, "key-$i", "message-$i")
            producer.send(record)
            println("✉️ Produced to input-topic: ${record.key()} -> ${record.value()}")
        }
        producer.flush()
    }

    // output-topic에서 메시지 수신
    val consumerProps = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ConsumerConfig.GROUP_ID_CONFIG, "output-consumer-${System.currentTimeMillis()}")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }
    KafkaConsumer<String, String>(consumerProps).use { consumer ->
        consumer.subscribe(listOf(outputTopic))
        println("⏳ Waiting for processed messages on output-topic...")
        while (consumer.assignment().isEmpty()) {
            consumer.poll(Duration.ofMillis(100))
        }
        val records = consumer.poll(Duration.ofSeconds(5))
        for (record in records) {
            println("✅ Stream output: ${record.key()} -> ${record.value()}")
        }
    }

    streams.close()
}

fun createTopicIfNotExists(bootstrapServers: String, topic: String) {
    val config = mapOf("bootstrap.servers" to bootstrapServers)
    AdminClient.create(config).use { admin ->
        val topics = admin.listTopics().names().get()
        if (!topics.contains(topic)) {
            val newTopic = NewTopic(topic, 1, 1.toShort())
            admin.createTopics(listOf(newTopic)).all().get()
            println("✅ Topic '$topic' created.")
        } else {
            println("✅ Topic '$topic' already exists.")
        }
    }
}