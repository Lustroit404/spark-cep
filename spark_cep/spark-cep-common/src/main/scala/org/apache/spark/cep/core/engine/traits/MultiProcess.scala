package org.apache.spark.cep.core.engine.traits
import org.apache.spark.sql.{DataFrame,SparkSession}
trait MultiProcess {
  def run(spark:SparkSession,
          inputTables:Seq[DataFrame],
          rule:String):DataFrame
}
