package org.apache.spark.cep.core.engine.traits
import org.apache.spark.sql.{DataFrame,SparkSession}
trait Accumulate {
  def run(spark:SparkSession,
          source:String,
          format:String,
          timeGranularity:Long,
          ):DataFrame
}
