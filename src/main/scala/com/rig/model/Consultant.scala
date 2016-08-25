package com.rig.model

/**
  * Created by gaur on 9/7/16.
  */
case class Consultant(_id:String,Profile:Profile)

case class Profile(IsExternalProfile : Boolean, Expertise : Int, ResumeLink :String, VideoLink : String, Summary : String)

