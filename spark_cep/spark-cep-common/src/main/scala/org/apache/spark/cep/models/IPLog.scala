package org.apache.spark.cep.models

case class IPLog (
                 UserId:Int,
                 ipAddress:String,
                 logTime:java.sql.Timestamp)
