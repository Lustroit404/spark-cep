package org.apache.spark.cep.core.rules

case class StateRule (
                     groupByKey:String,
                     stateType:String,
                     source:String,
                     format:String,
                     stateSizeLimit:Int,
                     condition:String,
                     stateTimeoutMs:Long,

                     )
