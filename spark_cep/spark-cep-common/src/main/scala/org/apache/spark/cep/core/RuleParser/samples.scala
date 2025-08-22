package org.apache.spark.cep.core.RuleParser
import org.apache.spark.sql._
class samples {
  val spark=SparkSession.builder().getOrCreate()
  import spark.implicits._
}
