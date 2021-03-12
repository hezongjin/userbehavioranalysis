package com.dadiyunwu.networkflow_analysis.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object FlinkHelper {

  def getKafkaConsumer(brokerId: String, topic: String) = {
    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerId)
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer")
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop).setStartFromEarliest()

    consumer
  }

}
