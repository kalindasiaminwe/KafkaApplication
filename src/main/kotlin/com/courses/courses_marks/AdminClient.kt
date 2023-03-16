package com.courses.courses_marks

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import java.util.Properties

class AdminClient(config: Config) {
    private var adminClient: AdminClient

    init {
        val properties = Properties().apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
            put(AdminClientConfig.CLIENT_ID_CONFIG, config.adminClientId)
            put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
        }

        adminClient = AdminClient.create(properties)
    }

    fun createTopic(topicName: String, numPartitions: Int, replicationFactor: Int){
        if (topicExists(topicName)) {
            throw Error("$topicName already exists")
        }

        val newTopic = NewTopic(topicName, numPartitions, replicationFactor)

        adminClient.createTopics(listOf(newTopic))
    }



    fun close() = adminClient.close()

    private fun topicExists(topic: String): Boolean{
        return  adminClient.listTopics().names().get().contains(topic)
    }

}