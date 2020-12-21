package com.cgy.realtime.app

import com.alibaba.fastjson.JSON
import com.cgy.gmall.common.Constant
import com.cgy.realtime.bean.OrderInfo
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import org.json4s.{CustomSerializer, DefaultFormats}

// 把Kafka数据，存入Hbase
object OrderApp_1 extends BaseApp {
  override val appName: String = "OrderApp"
  override val master: String = "local[2]"
  override val batchTime: Int = 3
  override val groupId: String = "OrderApp"
  override val topic: String = Constant.ORDER_INFO_TOPIC


  override def run(ssc: StreamingContext, sourceStream: DStream[String]): Unit = {
    sourceStream.map(json => JSON.parseObject(json, classOf[OrderInfo]))  // 把Kafka的json解析
      .foreachRDD(rdd => {
        import org.apache.phoenix.spark._
        // 把数据写入到 Phoenix 、
        // 现在Phoenix中创建表
        rdd.saveToPhoenix(
          "GMALL_ORDER_INFO",
          Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
        )
      })


  }
}

// json4s => json for scala
