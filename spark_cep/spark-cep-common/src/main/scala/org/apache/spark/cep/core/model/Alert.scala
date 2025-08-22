package org.apache.spark.cep.core.model
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
case class Alert(
                alertId:String,
                ruleName:String,
                matchedEvents:List[GenericEvent],
                alertTime:Timestamp,
                alertMessage:String
                )
