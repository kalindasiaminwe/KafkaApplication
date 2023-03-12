package com.courses.courses_marks

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.collections.HashMap


fun main(args: Array<String>){
    KotlinProducer("localhost:9092",).produce()
}
class KotlinProducer(brokers: String) {

//    private val logger = LoggerFactory.getLogger(javaClass)
    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        return KafkaProducer(props)
    }

    fun produce() {

        val courses = mutableListOf<Courses>()
        courses.add(Courses("science",78))
        courses.add(Courses("mathematics", 69))
        courses.add(Courses("english", 85))
        val json = """
          [
            {
              "course":"science",
              "marks":78
              
            },
            {
              "course":"mathematics",
              "marks":69
            },
            {
               "course":"mathematics",
               "marks":69
            
            }
          ]""".stripIndent()




        producer.send(ProducerRecord("inTopic", courses.toString()))
        producer.flush()

//        val coursesJson = JSONObject(courses).toString()
//
//        val producerOutput = producer.send(ProducerRecord("in", coursesJson))
//
//        producerOutput.get()
    }


}
//        kafkaTemplate.send("topicTwo", courses.toString(),json)
//        kafkaTemplate.flush()

//    val numbersMap = mapOf("one" to 1, "two" to 2, "three" to 3)
//    println(numbersMap.keys)
//    println(numbersMap.values)


//    fun produce() {
//        val courses = HashMap<String, Int>()
//        courses.put("Science",78);
//        courses.put("Mathematics",69);
//        courses.put("English",85);
//




