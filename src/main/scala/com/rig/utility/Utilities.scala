package com.rig.utility

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
/**
  * Created by gaur on 24/8/16.
  */
object Utilities {

  val confInfo = ConfigFactory.load()
  var url = confInfo.getString("spark_kafka.url")
  var tokenUrl = confInfo.getString("authorization.tokenUrl")
  var token = ""
  var checkpointdir = confInfo.getString("spark_kafka.checkPointDir")
  var masterUrl = confInfo.getString("spark_kafka.masterUrl")
  var appName = confInfo.getString("spark_kafka.appName")
  var brokers = confInfo.getString("spark_kafka.brokers")
  var zookeeper  = confInfo.getString("spark_kafka.zookeeper")
  var topics = confInfo.getString("spark_kafka.topics").split(",").toSet
  var batchDuration = confInfo.getString("spark_kafka.batchDuration")
  var cacheUrl = confInfo.getString("spark_kafka.cacheUrl")
  var dbName = confInfo.getString("spark_kafka.dbName")
  var mongodb = confInfo.getString("mongo.db")
  var mongocol = confInfo.getString("mongo.collection")
  var mongoServer = confInfo.getString("spark_kafka.server")
  var mongoPort = confInfo.getString("spark_kafka.port")

  def quiet_logs():Unit =  {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }


  def setVariableValues(confInfo:Config): Unit =
  {
    dbName = confInfo.getString("spark_kafka.dbName")
    println("****************************compatibility match url is " + url)
    checkpointdir = confInfo.getString("spark_kafka.checkPointDir")
    masterUrl = confInfo.getString("spark_kafka.masterUrl")
    appName = confInfo.getString("spark_kafka.appName")
    brokers = confInfo.getString("spark_kafka.brokers")
    zookeeper  = confInfo.getString("spark_kafka.zookeeper")
    topics = confInfo.getString("spark_kafka.topics").split(",").toSet
    batchDuration = confInfo.getString("spark_kafka.batchDuration")
    println("*******************************caching url is " + cacheUrl)
  }
}
