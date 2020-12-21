package com.cgy.realtime.app

import com.cgy.gmall.common.Constant
import com.cgy.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

abstract class BaseApp {
  val appName:String
  val master:String
  val batchTime:Int
  val groupId:String
  val topic:String
  def main(args: Array[String]): Unit = {
    // 通过流的方式去消费
    // 1. 创建一个StreamingContext
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchTime))
    // 2. 从kafka获取流
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, groupId, topic)

    // 具体的业务
    run(ssc,sourceStream)

    // 3. 启动上下文
    ssc.start()
    // 5. 阻止主线程退出，防止流出关闭
    ssc.awaitTermination()
  }

  def run(ssc: StreamingContext,sourceStream: DStream[String] )
}






















