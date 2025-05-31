package org.Lustroit404
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode,StreamingQuery}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StateSpec
object SparkMySQLCEP {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("MySQL-CEP")
      .master("local[*]")
      .config("spark.sql.streaming.stateStore.providerClass","org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()
    val orderStream=spark.readStream
      .format("jdbc")
      .option("url",
      "jdbc:mysql://localhost:3308/test")
      .option("dbtable","")
      .option("user","hive")
      .option("password","hive")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .load()
    val ordersDF=orderStream.select(
      col("user_id").cast("integer"),
      col("swim_time").cast("timestamp"),
      col("amount").cast("decimal(10,3)"),
      col("depth").cast("decimal(10,2)")
    ).filter("swim_time IS NOT NULL")

    val windowedAnalysis=ordersDF.withWatermark("swim_time","10 minutes")
      .groupBy(col("user_id"),window(col("swim_time"),"30 minutes","5 minutes"))
      .agg(
        count("*").alias("total_orders"),
        min("swim_time").alias("first_swim_time"),
        max("swim_time").alias("last_swim_time")
      )
      .where("total_swim>=3")
      .select(
        col("user_id"),
        col("window.start").alias("windows_start"),
        col("window.end").alias("window_end"),
        col("total_orders"),
        col("first_swim_time"),
        col("last_swim_time")
      )
    val query:StreamingQuery=windowedAnalysis.writeStream
      .outputMode(OutputMode.Append())
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3308/Lustroit404")
      .option("dbtable","abnormal_behavior")
      .option("user","hive")
      .option("password","hive")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("batchSize",500)
      .option("isolationLevel","TRANSACTION_READ_COMMITTED")
      .start()

    query.awaitTermination()
  }


}
