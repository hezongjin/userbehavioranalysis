package com.dadiyunwu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object KafkaInput {
  def main(args: Array[String]): Unit = {


    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dev01:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](prop)

    val strings = Source.fromFile("E:\\learn\\userbehavioranalysis\\hotitemsanalysis\\src\\main\\resources\\UserBehavior.csv").getLines()

    var cnt = 0
    for (elem <- strings) {
      val record = new ProducerRecord[String, String]("hotitems", elem)
      producer.send(record)
      cnt += 1
      if (cnt % 1000 == 0) {
        producer.flush()
        Thread.sleep(1000)
      }
    }

    producer.close()
  }
}
