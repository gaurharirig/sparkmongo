package com.rig.main

import java.io.File

import com.rig.spark.SparkKafkaMongo
import com.typesafe.config.ConfigFactory

/**
  * Created by gaur on 3/8/16.
  */
object SparkMain {

  def main(args:Array[String]):Unit={

    if ( args.length > 0 && args(0) != null) {
      val confInfo = ConfigFactory.parseFile(new File(args(0)))
      SparkKafkaMongo.setVariableValues(confInfo)
      println(SparkKafkaMongo.masterUrl,SparkKafkaMongo.dbName,SparkKafkaMongo.cacheUrl,SparkKafkaMongo.checkpointdir,SparkKafkaMongo.url)
      SparkKafkaMongo.processSparkStreaming()
    }else {
      println("please enter valid path for configuration file")
    }

  }
}
