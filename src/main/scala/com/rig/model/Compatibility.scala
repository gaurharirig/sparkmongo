package com.rig.model
import com.mongodb.casbah.Imports.ObjectId
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._Option

/**
  * Created by gaur on 25/8/16.
  */
case class Compatibility (_id:String,insertedOrUpdatedEntityName:String,isInsert:Boolean) extends BaseModel


case class CompatibilityParams(_id:Option[String]=None, insertedOrUpdatedEntityName:Option[String] = None, isInsert:Option[Boolean] = None)