package com.cgy.realtime.util

import java.io.InputStream
import java.util.Properties

object PropertiesUtil {
  private val is: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
  private val properties: Properties = new Properties()
  properties.load(is)
  def getProperty(propertyName:String): String = properties.getProperty(propertyName)

  def main(args: Array[String]): Unit = {
    println(getProperty("kafka.broker.list"))
  }
}
