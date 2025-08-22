package org.apache.spark.cep.core.engine
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.cep.core.engine.traits.MultiProcess
class SparkMultiProcess extends MultiProcess {
   override def run(spark:SparkSession,
                    inputTables:Seq[DataFrame],
                    rule:String):DataFrame={
     inputTables.zipWithIndex.foreach{
       case(df,idx)=>
         df.createOrReplaceTempView(s"multiprocess_tbl_$idx")
     }
     spark.sql(rule)
   }
}
