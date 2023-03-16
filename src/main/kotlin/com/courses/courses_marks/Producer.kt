package com.courses.courses_marks

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class Producer(config: Config) {

    private val producer: KafkaProducer<String, String>

    init {
        val properties = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, "10")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        }

        producer = KafkaProducer<String, String>(properties)
    }

    fun sendpayload(topic: String, payload:String){
        val producerRecord = ProducerRecord<String, String>(topic, payload)

        producer.send(producerRecord)
        producer.flush()
    }

}