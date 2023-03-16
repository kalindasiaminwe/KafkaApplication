package com.courses.courses_marks

data class Config(
    val bootstrapServer: String = "localhost:9092",
    val adminClientId: String = "admin-client-id",
    val streamsApplicationId: String = "streams-application-id",
    val sourceTopic: String = "input",
    val targetTopic: String = "output",
    val numPartitions: Int = 1,
    val replicationFactor: Int = 1

)
