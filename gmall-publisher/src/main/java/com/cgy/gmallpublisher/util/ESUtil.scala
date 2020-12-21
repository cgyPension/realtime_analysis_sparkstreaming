package com.cgy.gmallpublisher.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import scala.collection.JavaConverters._


object ESUtil {

  // 1. 先创建es客户端
  val factory: JestClientFactory = new JestClientFactory
  val uris = ("http://hadoop102:9200" :: "http://hadoop103:9200" :: "http://hadoop104:9200" :: Nil).asJava
  val conf = new HttpClientConfig.Builder(uris)
    .maxTotalConnection(100)
    .connTimeout(10000)
    .readTimeout(10000)
    .build()

  factory.setHttpClientConfig(conf)

  def getClient() = factory.getObject

  def getQueryDSL(date:String,keyword:String,startpage:Int,size:Int)={
    s"""
       |{
       |  "query": {
       |    "bool": {
       |      "must": [
       |        {"term": {
       |          "dt": {
       |            "value": "${date}"
       |          }
       |        }},
       |        {"match": {
       |          "sku_name": {
       |            "operator": "and",
       |            "query": "${keyword}"
       |          }
       |        }}
       |      ]
       |    }
       |  },
       |  "aggs": {
       |    "group_by_gender": {
       |      "terms": {
       |        "field": "user_gender",
       |        "size": 2
       |      }
       |    },
       |    "group_by_age": {
       |      "terms": {
       |        "field": "user_age",
       |        "size": 100
       |      }
       |    }
       |  },
       |  "size": ${size},
       |  "from": ${(startpage-1)*size}
       |}
       |""".stripMargin
  }
}



