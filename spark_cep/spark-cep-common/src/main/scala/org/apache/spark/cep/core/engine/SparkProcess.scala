package org.apache.spark.cep.core.engine
import org.apache.spark.sql._
import org.apache.spark.cep.core.engine.traits.Process
class SparkProcess extends Process {
  override def run(spark:SparkSession,inputTables:Seq[DataFrame],rule:String):DataFrame={
    inputTables.zipWithIndex.foreach{
      case(df,idx)=>
        df.createOrReplaceTempView(s"process_tbl_$idx")
    }
    spark.sql(rule)
  }
}
