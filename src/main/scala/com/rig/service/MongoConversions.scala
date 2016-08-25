package com.rig.service
import com.mongodb.casbah.Imports._
import com.novus.salat._
import com.novus.salat.global._
import scala.language.implicitConversions
/**
  * Created by gaur on 25/8/16.
  */
class MongoConversions[T,T1] {

    implicit def paramsToDBObject(params:T): DBObject =
      grater[T].asDBObject(params)

    implicit def classToDBObject(c:T1): DBObject =
      grater[T1].asDBObject(c)

}
