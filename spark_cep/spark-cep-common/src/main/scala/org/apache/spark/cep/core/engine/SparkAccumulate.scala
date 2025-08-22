package org.apache.spark.cep.core.engine
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.cep.core.engine.traits.Accumulate
import org.apache.spark.cep.core.utils.{KafkaConfig,jdbcConfig}
class SparkAccumulate extends Accumulate {
  override def run(spark:SparkSession,
                   source:String,
                   format:String,
                   timeGranularity:Long,
                   ):DataFrame={
    val df=format match{
      case "csv"=>
        spark.read.format("csv").load(source)
      case "txt"=>
        spark.read.text("source")
      case "parquet"=>
        spark.read.format("parquet").load(source)
      case "kafka"=>
        //like: kafka://=
        def parseKafkaConfig(source:String):KafkaConfig={
          val Array(brokers,topics)=source.split("\\?")
          val Brokers=brokers.split("://").last
          val Topics=topics.split("=").last.split(",").toList

          KafkaConfig(Brokers,Topics)
        }
        val kafkaConfig=parseKafkaConfig(source)
        spark.read.format("kafka")
          .option("kafka.bootstrap.servers",kafkaConfig.brokers)
          .option("subscribe",kafkaConfig.topics.mkString(","))
          .load()


      case "jdbc"=>
        def parseJDBCConfig(source:String):jdbcConfig={
          val Array(_,urlAndTable)=source.split(":",2)
          //split out into like "mysql://ip:port/table"

          val url=s"jdbc:$urlAndTable"

          val table=urlAndTable.split("/").last

          jdbcConfig(url,table)
        }
        val JdbcConfig=parseJDBCConfig(source)
        spark.read.format("jdbc")
          .option("url",JdbcConfig.url)
          .option("dbtable",JdbcConfig.table)
          .load()
      case _ => throw new IllegalArgumentException(s"unsupported format:$format")

    }
    //you need to write a some data within your  or print e.stacktrace()
    df.createOrReplaceGlobalTempView("accumulate_temp")
    val result=spark.sql(
      s"""select * from (select *,window(event_time,'$timeGranularity') as time_window from accumulate_temp)""")

    if (format.nonEmpty && format!="parquet"){
      result.write.format(format).mode("overwrite").save(s"$source")
    }
    result
  }

}
