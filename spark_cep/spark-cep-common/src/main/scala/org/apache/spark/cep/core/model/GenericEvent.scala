package org.apache.spark.cep.core.model
import org.apache.spark.sql.types._
import java.sql.Timestamp
case class GenericEvent(
                       eventID:String,
                       eventTime:Timestamp,
                       key:String,
                       fields:Map[String,Any]
                       )
