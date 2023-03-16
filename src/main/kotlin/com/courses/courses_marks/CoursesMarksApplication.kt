package com.courses.courses_marks

import org.apache.kafka.streams.StreamsBuilder



fun main() {
	val config = Config()

	val sourceTopic = config.sourceTopic
	val targetTopic = config.targetTopic
	val numPartitions = config.numPartitions
	val replicationFactor = config.replicationFactor

	val payload = Utils.jsonStringPayload

	try {
		val adminClient = AdminClient(config)

		adminClient.createTopic(sourceTopic, numPartitions, replicationFactor)
		adminClient.createTopic(targetTopic, numPartitions, replicationFactor)
		adminClient.close()

		val producer = Producer(config)

		producer.sendpayload(sourceTopic, payload)

		val builder = StreamsBuilder()

		builder.stream<String, String>(sourceTopic)
			.mapValues { value ->
				DataParser.sumMarks(value)
			}
			.peek { _ , value -> println("Total sum is $value") }
			.to(targetTopic)

		val streams = Streams(config, builder)

		streams.start()

	} catch (e: Exception) {
		println("Something went wrong: ${e.message}")
	}
}
