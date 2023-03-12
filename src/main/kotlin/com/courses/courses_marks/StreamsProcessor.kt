package com.courses.courses_marks
//
//
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*
import java.util.stream.Collectors

//
fun main (args: Array<String>){

    StreamsProcessor().process()
}


class StreamsProcessor {



    fun process() {
        val streamsBuilder = StreamsBuilder()

        val courseJsonStream = streamsBuilder
            .stream("inTopic", Consumed.with(Serdes.String(), Serdes.String()))

        courseJsonStream.mapValues { value, _ ->
            val objectMapper = jacksonObjectMapper()
            val courses: MutableList<Courses> = value.let { objectMapper.readValue(it) }
            val integerList = courses.stream().map { v -> v.marks }.collect(Collectors.toList())

            val sum = integerList.stream().reduce(0) {aa: Int, bb:Int -> aa + bb}
            return@mapValues sum.toString()
        }.peek{ _: String, value:String -> println("The sum of the course marks is $value") }.to("outTopic")

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["application.id"] = "StreamsAPI"
        props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

        val streams = KafkaStreams(topology, props)
        streams.start()



//        val courseStream: KStream<String, Courses> = courseJsonStream.mapValues { value, _ ->
//            val course = jsonMapper.readValue(value, Courses::class.java)
//            course
//        }





    }

}