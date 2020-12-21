package com.cgy

import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.cgy.gmall.common.Constant
import com.google.protobuf.ByteString

import scala.util.Random

object CannalClient {

  import scala.collection.JavaConverters._

  def handleRowData(tableName: String, eventType: CanalEntry.EventType, rowDataList: util.List[CanalEntry.RowData]): Unit = {
    if (!rowDataList.isEmpty && tableName == "order_info" && eventType == EventType.INSERT) {
        handleData(rowDataList,Constant.ORDER_INFO_TOPIC)
      }else if (!rowDataList.isEmpty && tableName == "order_detail" && eventType == EventType.INSERT) {
      handleData(rowDataList, Constant.ORDER_DETAIL_TOPIC)
    }
  }

  def handleData(rowDataList: util.List[CanalEntry.RowData], topic: String) = {
      for (rowData <- rowDataList.asScala) {
        val columns: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
        val obj: JSONObject = new JSONObject()
        for (column <- columns.asScala) {
          val key: String = column.getName
          val value: String = column.getValue
          obj.put(key,value)
        }
          // 发送到kafka
        // 模拟网络延迟
       /* new Thread(){
          override def run(): Unit = {
              Thread.sleep(new Random().nextInt(10)*1000)
              MyKafkaUtil.sendToKafka(topic,obj.toJSONString)
          }
        }.start()*/

        MyKafkaUtil.sendToKafka(topic,obj.toJSONString)

      }
  }



  def main(args: Array[String]): Unit = {
    // 1. 创建能连接到 Canal 的连接器对象
    val addr: InetSocketAddress = new InetSocketAddress("hadoop102", 11111)
    val conn: CanalConnector = CanalConnectors.newSingleConnector(addr, "example", "", "")

    // 2. 连接到 Canal
    conn.connect()
    // 3. 监控指定的表的数据的变化
    conn.subscribe("gmall0523.*")

    // TODO 解析数据

    //  从服务器读取数据
    while (true) {
      // 4. 获取消息  (一个消息对应 多条sql 语句的执行)
      val msg: Message = conn.get(100) // 一次最多获取 100 条 sql
      // 5. 一个消息对应多行数据发生了变化, 一个 entry 表示一条 sql 语句的执行
      val entries: util.List[CanalEntry.Entry] = msg.getEntries
      if (entries.size() > 0) {
        // 6. 遍历每行数据
        for (entry <- entries.asScala) {
          // 7. EntryType.ROWDATA 只对这样的 EntryType 做处理
          if (entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA) {
            // 8. 获取到这行数据, 但是这种数据不是字符串, 所以要解析
            val storeValue: ByteString = entry.getStoreValue
            val rowChange: RowChange = RowChange.parseFrom(storeValue)
            val rowDatasList: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
            // 9.定义专门处理的工具类: 参数 1 表名, 参数 2 事件类型(插入, 删除等), 参数 3: 具体的数据
            handleRowData(entry.getHeader.getTableName, rowChange.getEventType, rowChange.getRowDatasList)
          }

        }
      } else {
        println("没有拉到数据，2s之后继续拉...")
        Thread.sleep(2000)
      }
    }





  }
}
