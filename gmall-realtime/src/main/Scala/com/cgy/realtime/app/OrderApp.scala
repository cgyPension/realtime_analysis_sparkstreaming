package com.cgy.realtime.app
import com.alibaba.fastjson.JSON
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.cgy.gmall.common.Constant
import com.cgy.realtime.bean.OrderInfo
import org.json4s.{CustomSerializer, DefaultFormats, JDouble, JString}
import org.json4s.jackson.JsonMethods

 object OrderApp extends BaseApp {
   override val appName: String =  "OrderApp"
   override val master: String = "local[2]"
   override val batchTime: Int = 3
   override val groupId: String = "OrderApp"
   override val topic: String = Constant.ORDER_INFO_TOPIC

   object StringToDouble extends CustomSerializer[Double](format =>(
     {
       case JString(s) => s.toDouble
     },
     {
       case d:Double => JDouble(d)
     }
   ))

   override def run(ssc: StreamingContext, sourceStream: DStream[String]): Unit =  {
    // val orderInfoStream: DStream[OrderInfo] = sourceStream.map(json => JSON.parseObject(json, classOf[OrderInfo]))

     val orderInfoStream: DStream[OrderInfo] = sourceStream.map(json => {
       implicit val f = org.json4s.DefaultFormats + StringToDouble
       JsonMethods.parse(json).extract[OrderInfo]
     })

     orderInfoStream.print()
   }
 }

// json4s => json for scala
