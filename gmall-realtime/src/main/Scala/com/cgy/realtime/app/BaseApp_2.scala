package com.cgy.realtime.app

import com.cgy.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 同时消费多个topic，得到多个流 一个topic对应一个流
abstract class BaseApp_2 {
  val appName:String
  val master:String
  val batchTime:Int
  val groupId:String
  val topics:Set[String] // 定义多个topic，会得到多个流

  def main(args: Array[String]): Unit = {
    // 通过流的方式去消费
    // 1. 创建一个StreamingContext
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchTime))
    val topicAndStream: Map[String, DStream[String]] = topics.map(topic => {
      (topic, MyKafkaUtil.getKafkaStream(ssc, groupId, topic))
    }).toMap

    run(ssc,topicAndStream)
    // 3. 启动上下文
    ssc.start()
    // 5. 阻止主线程退出，防止流出关闭
    ssc.awaitTermination()
  }

  def run(ssc: StreamingContext,topicAndStream: Map[String, DStream[String]])
}






















