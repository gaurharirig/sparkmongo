package com.rig.spark

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{KafkaUtils, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by gaur on 16/7/16.
  */
object SparkOffsetCheck {

/*  def main(args: Array[String]): Unit = {
    getOffset()
  }*/

  def getOffset(): Unit = {
/*    val sconf = new SparkConf()
    sconf.setMaster("local[4]")
    sconf.setAppName("testkafka")

    val sparkContext = new SparkContext(sconf)
    val ssc = new StreamingContext(sparkContext, Seconds(6))
    ssc.checkpoint("checkdir")*/

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092","auto.offset.reset"->"smallest")

    val topics = Set("oplogger11")

    //val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //stream.map( x => x._2).print()

    def createStreamingContext():StreamingContext = {
      val sparkConf = new SparkConf().setAppName("testKafka").setMaster("local[4]")
      val sparkContext = new SparkContext(sparkConf)
      @transient val streamingContext = new StreamingContext(sparkContext, Seconds(6))
      streamingContext.checkpoint("checkdir11")




      var offsetRanges = Array[OffsetRange]()

      val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)

      directKafkaStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }.map { rdd => rdd
      }.foreachRDD { rdd =>
        for (o <- offsetRanges) {
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
        rdd.collect().foreach(println)
        println(rdd.count())
      }

      for (o <- offsetRanges) {
        println("offset is" + o.toString())
      }
      streamingContext
    }
    val ssccheck = StreamingContext.getOrCreate("checkdir11",createStreamingContext)

    ssccheck.start()
    ssccheck.awaitTermination()
  }



}
