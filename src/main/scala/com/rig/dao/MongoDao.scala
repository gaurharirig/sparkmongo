package com.rig.dao
import com.mongodb.casbah.Imports._
import com.novus.salat.dao.SalatDAO
import com.novus.salat.global._
import com.rig.model.BaseModel
import com.rig.utility.Utilities

/**
  * Created by gaur on 24/8/16.
  */
class MongoDao[T,T1] extends SalatDAO[T, T1](collection= MongoConnection()(Utilities.mongodb)(Utilities.mongocol)){

}
