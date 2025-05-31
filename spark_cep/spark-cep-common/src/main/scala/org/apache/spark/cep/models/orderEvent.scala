package org.apache.spark.cep.models

import scala.util._
import java.sql._

case class orderEvent(orderId:String,
                        userId:Int,
                        swim_Time:java.sql.Timestamp,
                        amount:Double)