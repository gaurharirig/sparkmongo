package com.rig.service

import com.google.gson.Gson
import com.rig.model.Compatibility

import scala.util.parsing.json.JSON
import scalaj.http.{Http, HttpOptions}



case class CacheUpdate(_id:String,UpdatedEntityName:String)

/**
  * Created by gaur on 9/7/16.
  */
object CreateCompatibility {


  def setAuthCompatibility(compatibility: Compatibility, url:String, token:String,timeout:Int): String =
  {
    try {
      val spockAsJson = new Gson().toJson(compatibility)
      val result = Http(url).postData(spockAsJson)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .header("Authorization", "Bearer " + token)
        .option(HttpOptions.readTimeout(timeout)).asString
        result.statusLine
    } catch{
      case ex: Exception => {
          "exception occured "+ ex.getStackTraceString + setCompatibility(compatibility,url,token,timeout)
        }
    }
  }

  def setCompatibility(compatibility: Compatibility, url:String, token:String,timeout:Int): String =
  {
    try {
      val spockAsJson = new Gson().toJson(compatibility)
      val result = Http(url).postData(spockAsJson)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(timeout)).asString
      if(result.code != 204)
        {

        }
      result.statusLine
    } catch{
      case ex: Exception => {
        "exception occured "+ ex.getStackTraceString + setCompatibility(compatibility,url,token,timeout)
      }
    }
  }

  def getAuthenticated(url:String,username:String,password:String,grant_type:String): String =
  {
    try {
      val token = Http(url).postForm(Seq("username" -> username, "password" -> password, "grant_type" -> grant_type)).asString.body
      val tokenObject = JSON.parseFull(token)
      val data = tokenObject.getOrElse("access_token", "not found").asInstanceOf[Map[String, String]]
      data.getOrElse("access_token", "not_found")
    }catch{
      case ex:Exception =>{
        "exception occured "+ ex.getStackTraceString + getAuthenticated(url,username,password,grant_type)
      }
    }
  }

  def updateCache(cachearg:CacheUpdate,cacheUrl:String,timeout:Int): Unit =
  {
    try {
      val spockAsJson = new Gson().toJson(cachearg)
      val result = Http(cacheUrl).postData(spockAsJson)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(timeout)).asString
      result.statusLine
    } catch{
      case ex: Exception => {
        "exception occured "+ ex.getStackTraceString + updateCache(cachearg,cacheUrl,timeout)
      }
    }

  }

}
