package com.cgy.realtime.app

import java.lang

import com.alibaba.fastjson.JSON
import com.cgy.realtime.util.{MyKafkaUtil, MyRedisUtil}

import com.cgy.gmall.common.Constant
import com.cgy.realtime.bean.StartupLog
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


object DauApp {
  def main(args: Array[String]): Unit = {
    // 通过流的方式去消费
    // 1. 创建一个StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 2. 从kafka获取流
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, "DauApp", Constant.STARTUP_TOPIC)
    // 3. 对流做各种转换操作，输出（print、foreachPartitions）
    // 3.1 对数据做封装，封装到样例类中
    val startupLogStream: DStream[StartupLog] = sourceStream
      .map(jsonString => JSON.parseObject(jsonString, classOf[StartupLog]))

   /* // 3.2 使用redis清洗
    val filteredStartupLogStream: DStream[StartupLog] = startupLogStream.filter(log => {
      println("过滤前：" + log)
      // 1. 现货区redis的客户端
      val client: Jedis = MyRedisUtil.getJedisClient
      // 2. 写到set： 返回1表示第一次，返回0表示不是第一次
      val r: lang.Long = client.sadd(s"dau:${log.logDate}", log.mid)
      client.close()
      // 3. 把返回值是1的log保留
      r == 1
    })*/


    // iter.tolist 很容易造成内存溢出OOM，可以用treeset

/*    val filteredStartupLogStream: DStream[StartupLog] = startupLogStream.mapPartitions((startupIt: Iterator[StartupLog]) => {
      val client: Jedis = MyRedisUtil.getJedisClient
      val result: Iterator[StartupLog] = startupIt.filter(log => {
        client.sadd(s"dau:${log.logDate}", log.mid) == 1
      })
      client.close()
      result
    })*/

    // 1. driver
    val filteredStartupLogStream: DStream[StartupLog] = startupLogStream.transform( rdd => {
      // 2. driver
     rdd.mapPartitions(startupIt => {
       // 3. executor
       val client: Jedis = MyRedisUtil.getJedisClient
       val result: Iterator[StartupLog] = startupIt.filter(log => {
         client.sadd(s"dau:${log.logDate}", log.mid) == 1 // 写到set： 返回1表示第一次，返回0表示不是第一次
       })
       client.close()
       result
     })

    })


    // 3.2 把每个设备每天的第一次启动记录写入写入到 Phoenix(HBase)
    filteredStartupLogStream.foreachRDD(rdd =>{

      println("-------------------------------------------")
      println(s"Time: ${System.currentTimeMillis()}")
      println("-------------------------------------------")

      import org.apache.phoenix.spark._
      // 参数1: 表名  参数2: 列名组成的 seq 参数 zkUrl: zookeeper 地址
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))
    })

    // 3. 启动上下文
    ssc.start()
    // 5. 阻止主线程退出，防止流出关闭
    ssc.awaitTermination()
    //
  }
}

/*
对流的数据去重, 一定要借助于外部存储: redis  set
org.apache.phoenix.jdbc.PhoenixDriver
 */