package com.cgy.realtime.app

import com.alibaba.fastjson.JSON
import com.cgy.gmall.common.Constant
import com.cgy.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.cgy.realtime.util.{ESUtil, MyRedisUtil}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

// TODO 需求：根据条件来灵活分析用户的购买行为
object SaleDetailApp extends BaseApp_2 {
  override val appName: String = "SaleDetailApp"
  override val master: String = "local[2]"
  override val batchTime: Int = 3
  override val groupId: String = "SaleDetailApp"
  override val topics: Set[String] = Set(Constant.ORDER_INFO_TOPIC, Constant.ORDER_DETAIL_TOPIC)

  // 用窗口的方式 截留数据join
  def fullJoin_1(orderInfoStream: DStream[(String, OrderInfo)], orderDetailStream: DStream[(String, OrderDetail)]) = {
    // 1. 给流加window
    val orderInfoStreamWithWindow: DStream[(String, OrderInfo)] = orderInfoStream.window(Seconds(12), Seconds(3))
    val orderDetailStreamWithWindow: DStream[(String, OrderDetail)] = orderDetailStream.window(Seconds(12), Seconds(3))
    // 2. 用窗口join
    //join 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素连接在一起的(K,(V,W))的RDD
    val saleDetailStream: DStream[SaleDetail] = orderInfoStreamWithWindow
      .join(orderDetailStreamWithWindow)
      .map {
        case (orderId, (orderInfo, orderDetail)) =>
          SaleDetail()  // TODO join后的样例类 链式调用 妙
            .mergeOrderInfo(orderInfo)
            .mergeOrderDetail(orderDetail)
      }
      .mapPartitions((saleDetailIt: Iterator[SaleDetail]) => {  // 3. 去重
        val client: Jedis = MyRedisUtil.getJedisClient
        val result: Iterator[SaleDetail] = saleDetailIt.filter(SaleDetail => {
          // 1. set的key: 可以体现时间，但是会导致key过多，不好管理,不可以重复的集合
          1 == client.sadd("gmall0523", SaleDetail.order_id + ":" + SaleDetail.order_detail_id)
        })
        client.close()
        result
      })

    saleDetailStream.print()
    saleDetailStream
  }

        /*
      order_info信息如何缓存:
      key                                 value
      "order_info:" + order_id            order_info的信息变成json字符串存入
      order_info:1                        {"": "", ...}

      -------------

      order_detail信息如何缓存:(1)
      key                                                   value
      "order_detail:" + order_id + order_detail_id           json字符串


      val keys = keys("order_detail:" + order_id*")

      order_detail信息如何缓存:(2)
      key                                 value map
      "order_detail:" + order_id           field                  value
                                           order_detail_id        json字符串
       */
  //  缓存OrderInfo到redis
  // fastjson不能把样例类序列化 要用json4s
  def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo) = {
    // 两种写法
    /* implicit val f: DefaultFormats.type = org.json4s.DefaultFormats
     val json: String = Serialization.write(orderInfo)*/

    val json: String = Serialization.write(orderInfo)(org.json4s.DefaultFormats)
    // 添加数据的同时设置过期时间(也就是缓存时间)，单位是s
    client.setex("order_info:" + orderInfo.id, 30 * 60, json)
  }

  def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail) = {
    implicit val f: DefaultFormats.type = org.json4s.DefaultFormats
    val json: String = Serialization.write(orderDetail)
    client.hset("order_detail:"+ orderDetail.order_id,orderDetail.id,json)
  }

  def fullJoin_2(orderInfoStream: DStream[(String, OrderInfo)], orderDetailStream: DStream[(String, OrderDetail)]) = {
    // DStream[SaleDetail]
    orderInfoStream
      .fullOuterJoin(orderDetailStream) //(order_id,(order_id,orderDetail))
      .mapPartitions(it => {
        // 数据缓存redis 和del
        val client: Jedis = MyRedisUtil.getJedisClient
        val result: Iterator[SaleDetail] = it.flatMap {  // flatMap 返回值要是一个集合 空集合用 Nil
          // some some TODO 注意flatMap + 模式匹配的写法
          case (orderId, (Some(orderInfo), Some(orderDetail))) =>
            //  order_info 与 order_detail 是一对多的关系
            // 1. 缓存order_info
            cacheOrderInfo(client, orderInfo)
            // 2. TODO join后的样例类 链式调用 妙 要弄一个存储fulljoin后的容器
            val saleDetail: SaleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
            // 3. 去order_detail的缓存中找到这个order对应的所有order_detail
            if (client.exists("order_detail:" + orderId)) {
              val t: List[SaleDetail] = client
                .hgetAll("order_detail" + orderId)  // 默认是获得java的类型 这里是list的集合
                .asScala
                .map {
                  case (oderDetail, jsonString) =>
                    val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                    SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                }
                .toList :+ saleDetail  // 拼接回fulljoin的结果
              client.del("order_detail:" + orderId)
              t
            } else {
              saleDetail :: Nil
            }
          // some none
          case (orderId, (Some(orderInfo), None)) =>
            // 1. 缓存order_info
            cacheOrderInfo(client, orderInfo)
            // 3. 去order_detail的缓存中找到这个order对应的所有order_detail
            if (client.exists("order_detail:" + orderId)) {
              val t = client
                .hgetAll("order_detail:" + orderId)
                .asScala
                .map {
                  case (oderDetailId, jsonString) =>
                    val orderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                    SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                }
                .toList
              client.del("order_detail:" + orderId)
              t
            } else {
              Nil
            }
          // none some
          case (orderId, (None, Some(orderDetail))) =>
            // 1. 去orderInfo的缓存中查找对应的orderInfo信息
            if (client.exists("order_info:" + orderDetail.order_id)) {
              // 2. 如果找到，就join，返回join后的结果
              val orderInfoString: String = client.get("order_info:" + orderDetail.order_id)
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
              SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
            } else {
              // 3. 如果没有找到，返回空 null，并且把orderDetail信息写入到缓存
              cacheOrderDetail(client, orderDetail)
              Nil
            }


        }
        client.close()
        result
      })
  }




  /**
   * 把用户信息join到saleDetail中
   * 使用spark-sql来从msyql读数据 df/ds
   * 把流和df/ds转成rdd之后进行join
   *
   * @param ssc
   * @param saleDetailStream
   */
  def joinUser(ssc: StreamingContext, saleDetailStream: DStream[SaleDetail]) = {
    val spark: SparkSession = SparkSession.builder()
      .config(ssc.sparkContext.getConf)  // 获取旧的配置
      .getOrCreate()
      import spark.implicits._


    // 查询 Mysql 管理 UserInfo
    def readUserInfo(ids: List[String])= {
      spark
        .read
        .format("jdbc")
        .option("url","jdbc:mysql://hadoop102:3306/gmall0523?useSSL=false")
        .option("user","root")
        .option("password","123456")
        .option("query",s"select*from user_info where id in('${ids.mkString("','")}')")
        .load()
        .as[UserInfo]
        .rdd
        .map(userInfo => (userInfo.id,userInfo))
    }

    saleDetailStream.transform((rdd:RDD[SaleDetail])=>{
      // 每个批次一次
      rdd.cache() // rdd后面会使用多次，所以做缓存
      // 把这个批次中所有的user_id拿出来
      val ids: List[String] = rdd.map(_.user_id).collect().toSet.toList
      val userInfoRdd: RDD[(String, UserInfo)] = readUserInfo(ids)
      rdd.map(saleDetail => (saleDetail.user_id,saleDetail))
        .join(userInfoRdd)
        .map{
          case (userId,(saleDetail,userInfo))=>
            saleDetail.mergeUserInfo(userInfo)
        }

    })

  }

  // 要先在ES创建索引
  def writeToES(saleDetailWithUserStream: DStream[SaleDetail]): Unit = {
    saleDetailWithUserStream.foreachRDD(rdd=>{
      rdd.foreachPartition(sdIt => {
          ESUtil.insertBulk("gmall_sale_detail",sdIt.map(sd =>(sd.order_id+"_"+sd.order_detail_id,sd)))
      })
    })
  }

  override def run(ssc: StreamingContext, topicAndStream: Map[String, DStream[String]]): Unit = {
    // 从kafka获取两个订单流的业务数据
    val orderInfoStream: DStream[(String, OrderInfo)] = topicAndStream(Constant.ORDER_INFO_TOPIC)  // k v 类型可以直接通过key获取value
      .map(json => {
        val orderInfo = JSON.parseObject(json, classOf[OrderInfo])
        (orderInfo.id, orderInfo)
      })
    val orderDetailStream: DStream[(String, OrderDetail)] = topicAndStream(Constant.ORDER_DETAIL_TOPIC)
      .map(json => {
        val orderDetail: OrderDetail = JSON.parseObject(json, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })

    // 1 对两个流进行 join,返回值就是join后的流
    // 1.1 窗口方式 内连接join
    //    fulljoin_1(orderInfoStream,orderDetailStream)
    // 1.2 缓存 full join
    val saleDetailStream: DStream[SaleDetail] = fullJoin_2(orderInfoStream, orderDetailStream)
    // 2. join用户信息
    val saleDetailWithUserStream: DStream[SaleDetail] = joinUser(ssc, saleDetailStream)

    // TODO 测试
    // 3. 把数据写入到es
    writeToES(saleDetailWithUserStream)

  }
}

