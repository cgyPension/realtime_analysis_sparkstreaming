package com.cgy.realtime.app

import com.alibaba.fastjson.JSON
import com.cgy.gmall.common.Constant
import com.cgy.realtime.app.SaleDetailApp.cacheOrderInfo
import com.cgy.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.cgy.realtime.util.MyRedisUtil
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.Seconds
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

// TODO 需求：根据条件来灵活分析用户的购买行为
object SaleDetailApp2 extends BaseApp_2 {
  override val appName: String = "SaleDetailApp2"
  override val master: String = "local[*]"
  override val batchTime: Int = 3
  override val groupId: String = "SaleDetailApp2"
  override val topics: Set[String] = Set(Constant.ORDER_INFO_TOPIC,Constant.ORDER_DETAIL_TOPIC)

  // 用窗口的方式 截留数据join
  def fullJoin_11(orderInfoStream: DStream[(String, OrderInfo)], orderDetailStream: DStream[(String, OrderDetail)]) = {
    // 1 给流加window
    val orderInfoStreamWithWindow: DStream[(String, OrderInfo)] = orderInfoStream.window(Seconds(12), Seconds(3))
    val orderDetailInfoStreamWithWindow: DStream[(String, OrderDetail)] = orderDetailStream.window(Seconds(12), Seconds(3))
    // 2 真正的join 内连
    val saleDetailStream: DStream[SaleDetail] = orderInfoStreamWithWindow
      .join(orderDetailInfoStreamWithWindow)
      .map {
        case (orderId, (orderIndo, orderDetail)) =>
          SaleDetail()
            .mergeOrderInfo(orderIndo)
            .mergeOrderDetail(orderDetail)
      }
      .mapPartitions((saleDetail: Iterator[SaleDetail]) => {
        val client: Jedis = MyRedisUtil.getJedisClient
        // 3 去重 set不可重复集合
        val result: Iterator[SaleDetail] = saleDetail.filter(iter => {
          1 == client.sadd("gmall_orderid_orderdetail", iter.order_id + "_" + iter.order_detail_id)
        })
        client.close
        result
      })

      saleDetailStream.print()
      saleDetailStream
  }


  def cacheOrderInfo11(client: Jedis, orderInfo: OrderInfo)= {
   /* implicit val f: DefaultFormats.type = org.json4s.DefaultFormats
    val json: String = Serialization.write(orderInfo)*/

    val json: String = Serialization.write(orderInfo)(org.json4s.DefaultFormats)
    client.setex("order_info:"+orderInfo.id,30*60,json)
  }

  def fullJoin_22(orderInfoStream: DStream[(String, OrderInfo)], orderDetailStream: DStream[(String, OrderDetail)]) = {
    orderInfoStream
      .fullOuterJoin(orderDetailStream)  //()
      .mapPartitions(it=>{
        val client: Jedis = MyRedisUtil.getJedisClient
        it.flatMap{
          case (orderId, (Some(orderInfo), Some(orderDetail))) =>
            //
            cacheOrderInfo11(client,orderInfo)
            val saleDetail: SaleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
            if (client.exists("order_detail:"+orderId)) {
              val t: List[SaleDetail] = client
                .hgetAll("order_detail:" + orderId)
                .asScala
                .map {
                  case (orderDetail, jsonString) =>
                    val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                    SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                }
                .toList :+ saleDetail
              client.del("order_detail:"+orderId)
              t
            }else{
              saleDetail::Nil
            }

          case (orderId, (Some(orderInfo), None)) =>



          case (orderId, (None, Some(orderDetail))) =>



        }
      })
  }

  override def run(ssc: StreamingContext, topicAndStream: Map[String, DStream[String]]): Unit = {
    // 1 从kafka获取 两个订单的业务数据
    val orderInfoStream: DStream[(String, OrderInfo)] = topicAndStream(Constant.ORDER_INFO_TOPIC)
      .map(json => {
        val orderInfo: OrderInfo = JSON.parseObject(json, classOf[OrderInfo])
        (orderInfo.id, orderInfo)
      })
    val orderDetailStream: DStream[(String, OrderDetail)] = topicAndStream(Constant.ORDER_DETAIL_TOPIC)
      .map(json => {
        val orderDetail: OrderDetail = JSON.parseObject(json, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })

    // 2 对两个流进行 join,返回值就是join后的流
    // 2.1 窗口方式 内连接join
    //val saleDetailStream: DStream[SaleDetail] = fullJoin_11(orderInfoStream, orderDetailStream)

    // 2.2 缓存 full join
    fullJoin_22(orderInfoStream,orderDetailStream)

    // 3 join用户信息

    // 4 把数据写到es

  }
}
