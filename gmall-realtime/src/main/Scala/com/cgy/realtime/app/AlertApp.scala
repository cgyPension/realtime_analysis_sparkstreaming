package com.cgy.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.cgy.gmall.common.Constant
import com.cgy.realtime.bean.{AlertInfo, EventLog}
import com.cgy.realtime.util.ESUtil
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._
import ESUtil._
/**
 * 购物券风险预警分析：
 * 需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。
 * 同时达到以上要求则产生一条预警日志。 同一设备，每分钟只记录一次预警。
 *
 * 分析:
 * 同一设备   按照设备id分组
 * 5分钟内    window    窗口长度: 5分钟  滑动步长: 6s
 *
 * 三次及以上用不同账号登录并领取优惠劵
 *     统计领取优惠券的用户的个数
 * 并且在登录到领劵过程中没有浏览商品
 *     没有浏览商品: 5分种内没有出现浏览商品的行为
 *
 * es处理:
 *     同一设备，每分钟只记录一次预警。
 *         spark-Streaming不负责, 交给es来完成
 */

object AlertApp extends BaseApp {
  override val appName: String = "OrderApp"
  override val master: String = "local[2]"
  override val batchTime: Int = 3
  override val groupId: String = "OrderApp"
  override val topic: String = Constant.EVENT_TOPIC

  override def run(ssc: StreamingContext, sourceStream: DStream[String]): Unit = {
    // 添加窗口, 调整数据结构
    sourceStream.window(Seconds(5 * 60))
      .map(json => {
        val eventLog: EventLog = JSON.parseObject(json, classOf[EventLog])
        (eventLog.mid, eventLog)
      })
      .groupByKey() // 按照 mid 分组
      .map { // 预警的业务逻辑
        case (mid, it: Iterable[EventLog]) =>
          // 1. 统计领取优惠券用户的个数量
          val uids: util.HashSet[String] = new util.HashSet[String]()
          // 2. 当前设备所有的行为
          val events: util.ArrayList[String] = new util.ArrayList[String]()
          // 3. 优惠券对应的商品id
          val items: util.HashSet[String] = new util.HashSet[String]()

          // 4. 标记是否浏览过商品 默认没有
          var isBrowser = false
          // 遍历这个设备上5分钟内的所有事件日志
          breakable {
            it.foreach(eventLog => {
              // 保存所有的时间
              events.add(eventLog.eventId)
              eventLog.eventId match {
                // 记录下领优惠全的所有用户
                // case这样的模式匹配和if、else if差不多
                case "coupon" =>
                  uids.add(eventLog.uid) // 把领取优惠券的用户的id存起来
                  items.add(eventLog.itemId) // 存储优惠券对应的商品id
                case "clickItem" =>
                  isBrowser = true
                  break
                case _ =>
              }
            })
          }
          // 组合成元组(是否预警 true/false, 预警信息的封装)
          (!isBrowser && uids.size() >= 3,// 有时候模拟数据运气不是很好，可以改成2
            AlertInfo(mid, uids, items, events, System.currentTimeMillis())
          )
      }
      .filter(_._1) // (true,(..)) filter会把true的留下来  过滤掉不需要报警的信息
      .map(_._2)
      .foreachRDD(rdd => {
        // 把 预警写入到es
/*        rdd.foreachPartition((it:Iterator[AlertInfo])=>{
          ESUtil.insertBulk(
            "gmall_coupon_alert",
            it.map(info => (info.mid + ":" + info.ts / 1000 / 60, info))
          )
        })*/
        rdd.saveToES("gmall_coupon_alert")
      })

  }
}


// 在匿名函数里不要使用 return

/*
.foreachRDD(rdd => {
println("xxxx")
// 把 预警写入到es
rdd.foreachPartition((it:Iterator[AlertInfo])=>{
ESUtil.insertBulk(
"gmall_coupon_alert",
it.map(info => (info.mid + ":" + info.ts / 1000 / 60, info))
)
})
//rdd.saveToES("gmall_coupon_alert")
})*/
