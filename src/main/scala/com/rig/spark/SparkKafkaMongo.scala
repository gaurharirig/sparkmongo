package com.rig.spark

import com.rig.model.Compatibility
import com.rig.service.{CacheUpdate, CreateCompatibility}
import com.rig.utility.Utilities
import com.typesafe.config.{Config, ConfigFactory}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.parsing.json.JSON

case class AppURl(comUrl:String,cacheUrl:String)

/**
  * Created by gaur on 11/7/16.
  */
object SparkKafkaMongo {

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
  var timeout = confInfo.getInt("app_conf.timeout")

  def processSparkStreaming(): Unit =
  {
    val ssccheck = StreamingContext.getOrCreate(checkpointdir,getCheckPointContext)

    ssccheck.start()
    ssccheck.awaitTermination()
  }

  def getCheckPointContext():StreamingContext ={

    val sconf = new SparkConf().setMaster(masterUrl).setAppName(appName)

    val sparkContext = new SparkContext(sconf)
    sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext (sparkContext, Seconds (batchDuration.toInt) )
    ssc.checkpoint (checkpointdir)

    val kafkaParams = Map ("metadata.broker.list" -> brokers, "zookeeper.connect" -> zookeeper)

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (ssc, kafkaParams, topics)


    stream.checkpoint(new Duration(5000))

    stream.map(x => print("\n" +
      "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" +
      "\n" +"++++++++++++++++++++++++" + processMessage (x._2, url, token,cacheUrl,dbName) +"+++++++++++++++++++" + "\n"
      +"++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" +
      "\n" )).print()

    ssc
  }

  def processMessage(msg:String, argComUrl:String, token:String, argCacheUrl:String,argDbName:String): String =
  {
    try {
      val mapData = JSON.parseFull(msg).getOrElse("ns", "key not found").asInstanceOf[Map[String, String]];
      val entityType = mapData("ns")
      val dbnamen = entityType.split('.')
      val isInsert = true
      val entityTypeNew = dbnamen(1)
      val urls = getProcessUrl(confInfo,dbnamen(0))
      url = urls.comUrl
      println("****************************compatibility match url is " + url)
      val cacheUrl = urls.cacheUrl
      if ((entityType.contains("Job") || entityType.contains("Consultant")) && mapData.contains("id")) {
        val compat = new Compatibility(mapData("id"), entityTypeNew, isInsert)
        "compatibility match called with status " + CreateCompatibility.setCompatibility(compat, url, token,timeout)
      } else {
        CreateCompatibility.updateCache(new CacheUpdate(mapData("id"), entityTypeNew), cacheUrl,timeout)
        "updated caching successfully"
      }
    }catch{
      case ex:Exception => {
          "exception thrown by process message function " + ex.getStackTrace
      }
    }
  }

  def setVariableValues(confInfo:Config): Unit =
  {
    dbName = confInfo.getString("spark_kafka.dbName")
    val urls = getProcessUrl(confInfo,dbName)
    url = urls.comUrl
    println("****************************compatibility match url is " + url)
    checkpointdir = confInfo.getString("spark_kafka.checkPointDir")
    masterUrl = confInfo.getString("spark_kafka.masterUrl")
    appName = confInfo.getString("spark_kafka.appName")
    brokers = confInfo.getString("spark_kafka.brokers")
    zookeeper  = confInfo.getString("spark_kafka.zookeeper")
    topics = confInfo.getString("spark_kafka.topics").split(",").toSet
    batchDuration = confInfo.getString("spark_kafka.batchDuration")
    cacheUrl = urls.cacheUrl
    println("*******************************caching url is " + cacheUrl)
    timeout = confInfo.getInt("app_conf.timeout")
  }

  def getProcessUrl(confInfo:Config, argDbName:String): AppURl ={
    var out_url:AppURl = new AppURl(confInfo.getString("app_url.qacomurl"),confInfo.getString("app_url.qacacheurl"))
    out_url = argDbName match {
      case db if(db.contains(confInfo.getString("app_db.qadb"))) => new AppURl(confInfo.getString("app_url.qacomurl"),confInfo.getString("app_url.qacacheurl"))
      case db if(db.contains(confInfo.getString("app_db.devdb"))) => new AppURl(confInfo.getString("app_url.devcomurl"),confInfo.getString("app_url.devcacheurl"))
      case db if(db.contains(confInfo.getString("app_db.proddb"))) => new AppURl(confInfo.getString("app_url.prodcomurl"),confInfo.getString("app_url.prodcacheurl"))
      case db if(db.contains(confInfo.getString("app_db.qaproddb"))) => new AppURl(confInfo.getString("app_url.qaprodcomurl"),confInfo.getString("app_url.qaprodcacheurl"))
      case db if(db.contains(confInfo.getString("app_db.devtempdb"))) => new AppURl(confInfo.getString("app_url.devtempcomurl"),confInfo.getString("app_url.devtempcacheurl"))
      case db if(db.contains(confInfo.getString("app_db.demodb"))) => new AppURl(confInfo.getString("app_url.democomurl"),confInfo.getString("app_url.democacheurl"))
      case db if(db.contains(confInfo.getString("app_db.qademodb"))) => new AppURl(confInfo.getString("app_url.qademocomurl"),confInfo.getString("app_url.qademocacheurl"))

    }
    out_url
  }


}

