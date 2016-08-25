package com.rig.spark

import com.rig.model.{MatchRatingScale, Job}
import com.rig.service.{CreateCompatibility, Compatibility}
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext,Seconds}
import scala.util.parsing.json.JSON
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._

/**
  * Created by gaur on 7/7/16.
  */

object SparkKafka {

/*  def main(args:Array[String]): Unit =
  {
    val confData = ConfigFactory.load()
    val value = confData.getString("spark_kafka.url")
    val token = confData.getString("spark_kafka.checkPointDir")
    println(s"My secret value is $value and $token")
  }*/
 /* def main(args:Array[String]): Unit ={
    val token = "_d1QmW0Yt6y4IStzgoHQtVDejbzXF4E5hQQuyzbktPy15VZOSLaUwNHcdImVFNjOXsjUy2c1MgO2w-Xauq4CY8ItI2zNr3OVRmMmkNCwNt-TZ9JoM8UVfTIDOBPzkInWi_Kgi_iX1-6YzcKsa9_8FNVCM7k7A4SwKyd9OkWoBfM2lgBSdB43aLqS5MioYxUkcsxJ2dEOVgUqPIfI6anNaGZVuyuqkuPbMHkreQXjuB0DawLHyx2XRVf7ZfXjNStXTFY5WdeDhDERRr0LKARrepqYNZadqkJJJneQ4MfZk25xWF4zcYiDClESuuB7VEKETTkYav4dKTiUMkx7MInaiO8oQ8cLwEEfhdS7WmYsZ95iWJDuAKIdt8066GbSHQW9nPxHOc2RBxc8cqouMk7YbSZztCLdFjDtd3stagRB-HZWGuWooYZkW4VH4HcyUiYDcgCRtKsjKP4nHQfFTYLn5W3s0p5AxHkDnnTXykXR7SWzWrWHKI74jyl0za1W4w9hHbbWd3onKYZA5FBVXxO1XIwJ1uRyaVYF-IYfkQRD2OI";
    val url = "https://onblickdevapi.azurewebsites.net/Compatibility/CreateCompatibilityMatch"

    val sconf = new SparkConf()
    sconf.setMaster("local[4]")
    sconf.setAppName("testkafka")

    val sparkContext = new SparkContext(sconf)
    val ssc = new StreamingContext(sparkContext,Seconds(6))

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092","auto.offset.reset" -> "smallest","consumer.forcefromstart" -> "true" )

    val topics = Set("oplogger")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)

    //stream.map( x => x._2).print()

    var offsetRanges = Array[OffsetRange]()

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)

    /*directKafkaStream.transform { rdd =>
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

    for(o <- offsetRanges)
      {
        println("offset is"  + o.toString())
      }

    stream.foreachRDD(rdd => {
      //rdd.collect().foreach(println);
      rdd.map( x => {var result = JSON.parseFull(x);
              result
      }).map( mapData => {
       var mapDt =  mapData.get
        mapDt
      }).collect().foreach( x =>  println(x.asInstanceOf[Map[String,String]]("ns")))
      //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println(rdd.count())
    } )
    */

    stream.map(x =>  { var result = JSON.parseFull(x._2);
    var mapData = result.get.asInstanceOf[Map[String,String]]
      (x._1,mapData)}).print()

    stream.map( x => {
      var result = JSON.parseFull(x._2);
      var mapData = result.get.asInstanceOf[Map[String,String]]
      var ns = "Job"
      var isInsert = true
      if(mapData("ns").contains("Job"))
      {
        ns = "Job"
      }
      else if(mapData("ns").contains("Consultant"))
      {
        ns = "Consultant"
      }
      if(mapData("op").equals("u"))
      {
        isInsert = false
      }
      else if(mapData("op").equals("i"))
      {
        isInsert = true
      }
      if(mapData.contains("id")) {
        var compat = new Compatibility(mapData("id"), ns, isInsert)
        CreateCompatibility.setCompatibility(compat, url, token)
        (compat)
      }
    }).print()


      /*
      .collect().foreach( x =>  println(x.asInstanceOf[Map[String,String]]("ns")))
      //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println(rdd.count())
    } )
    */

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }*/

}
