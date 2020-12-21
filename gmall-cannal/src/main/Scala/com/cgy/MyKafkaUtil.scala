package com.cgy

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaUtil {

  private val props: Properties = new Properties()

  // Kafka服务端的主机名和端口号
  props.put("bootstrap.servers","hadoop102:9091,hadoop103:9092,hadoop104:9092")

  // key value 序列化
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  // 创建一个生产者
  private val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  def sendToKafka(topic:String,content:String): Unit ={
      producer.send(new ProducerRecord[String,String](topic,content))
  }
}