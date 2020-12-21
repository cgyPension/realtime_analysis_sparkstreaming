package com.cgy.realtime.util

import java.util

import com.cgy.realtime.bean.AlertInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._


object ESUtil {

  // 1. 先创建es客户端
  val factory: JestClientFactory = new JestClientFactory
  val uris =("http://hadoop102:9200" :: "http://hadoop103:9200" :: "http://hadoop104:9200" :: Nil).asJava
  val conf = new HttpClientConfig.Builder(uris)
    .maxTotalConnection(100)
    .connTimeout(10000)
    .readTimeout(10000)
    .build()

  factory.setHttpClientConfig(conf)


  def main(args: Array[String]): Unit = {

   /*  // 2. 写入数据
   val client: JestClient = factory.getObject
     val source =
       """
         |{
         | "name":"zs",
         | "age":20,
         | "sex":"male"
         |}
         |""".stripMargin
     val source1: User0523 = User0523("xihong", 12, "female")
     val source3: util.Map[String, Int] = Map("a" -> 5, "b" -> 99).asJava
     val index: Index = new Index.Builder(source)
       .index("user0523")
       .`type`("_doc")
       .build()
     client.execute(index)

     // 3. 关闭客户端
     client.shutdownClient()*/



  }

  // 单条插入数据
  def inserSingle(index:String,source:Object,id:String = null)={
    //建立连接
    val client: JestClient = factory.getObject
    //Builder中的参数，底层会转换为Json格式字符串，所以我们这里封装Document为样例类
    //当然也可以直接传递json
    val action = new Index.Builder(source)
      .index(index)
      .`type`("_doc")
      .id(id)
      .build()
    //execute的参数类型为Action，Action是接口类型，不同的操作有不同的实现类，添加的实现类为Index
    client.execute(action)
    //关闭连接
    client.shutdownClient()
  }

  // 向es插入多条数据
  def insertBulk(index: String, sources: Iterator[Object]) = {
    val client: JestClient = factory.getObject

    val bulkBuilder = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")
    /*sources.foreach(source => {
        val action = new Index.Builder(source).build()
        bulkBuilder.addAction(action)
    })*/
    /*sources
        .map(source => new Index.Builder(source).build())
        .foreach(bulkBuilder.addAction)*/

    sources
      .map { // 把所有的source变成action添加buck中
        //传入的是值是元组, 第一个表示id
        case (id: Any, source) =>
          new Index.Builder(source).id(id.toString).build()
        // 其他类型 没有id, 将来省的数据会自动生成默认id
        case source =>
          new Index.Builder(source).build()
      }
      .foreach(bulkBuilder.addAction)
    client.execute(bulkBuilder.build())
    client.shutdownClient()
  }

  implicit class RichEs(rdd:RDD[AlertInfo]){
    def saveToES(index: String) = {
      rdd.foreachPartition((it: Iterator[AlertInfo]) => {
        ESUtil.insertBulk(
          "gmall_coupon_alert",
          it.map(info => (info.mid + ":" + info.ts / 1000 / 60, info)))
      })
    }
  }
}
 case class User0523(name:String,age:Int,sex:String)


