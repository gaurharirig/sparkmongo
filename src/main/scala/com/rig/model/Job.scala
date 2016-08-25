package com.rig.model

/**
  * Created by gaur on 9/7/16.
  */
case class Job(_id:String,MatchRating:MatchRatingScale,Name:String)

case class MatchRatingScale(Location :String, Ratings : String, Skills : String, Experience : String, ProjectCount : String)

