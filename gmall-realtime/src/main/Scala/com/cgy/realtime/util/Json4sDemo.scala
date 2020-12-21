package com.cgy.realtime.util

import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}

object Json4sDemo {
  def main(args: Array[String]): Unit = {

    /*   val json=
         """
           |{"name:"zs","age":20}
           |""".stripMargin
     implicit val f = org.json4s.DefaultFormats
     println((JsonMethods.parse(json) \ "name").extract[String])
     println((JsonMethods.parse(json) \ "age").extract[Int])
     println(JsonMethods.parse(json).extract[User])*/

    // val user: User = User("lisi", 10)
    // val user = Map("a" -> 97, "b" -> 98)
    val user = List(30, 50, 70, 60, 10, 20)
    implicit val f: DefaultFormats.type = org.json4s.DefaultFormats
    val s: String = Serialization.write(user)
    println(s)

  }
}

case class User(name: String, age: Int)

/**
 * json 操作分两种
 * 1.解析
 *       反序列化
 *       json => 对象
 *
 *       在scala中，用fastjson解析一般问题不大
 *
 * 2.序列化
 *        对象 => json 字符串
 *
 *        在scala中，用fastjson序列化，做不到
 *
 * fastjson不能把样例类序列化
 */

