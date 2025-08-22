package org.apache.spark.cep.core.rules

case class WindowRule (
                      windowSizeMs:Long,
                      slideIntervalMs:Long,
                      watermarkDelayMs:Long,
                      groupByKey:String,
                      eventTimeField:String,
                      condition:String,
                      resultFields:List[String],
                      source:String,
                      format:String
                      )
