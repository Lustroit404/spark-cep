package org.apache.spark.cep
import org.apache.spark.cep.models._
import org.apache.spark.cep.utils.StateManager
import org.apache.spark.sql.{SparkSession,functions=>F}
import org.apache.spark.sql.streaming.OutputMode
object client {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StructuredCEP")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/cep-checkpoint")
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()

    import spark.implicits._

    // 模拟订单流（从Kafka读取，假设value为JSON）
    val ordersStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user_orders")
      .load()
      .select(F.from_json(F.col("value").cast("string"), schemaOf[OrderEvent]).as("data"))
      .select("data.*")

    // 模拟IP日志流
    val ipLogsStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user_ip_logs")
      .load()
      .select(F.from_json(F.col("value").cast("string"), schemaOf[IpLog]).as("data"))
      .select("data.*")

    // 1. 检测高频下单（30分钟内≥3单）
    val highFrequencyOrders = ordersStream
      .withWatermark("orderTime", "10 minutes") // 处理乱序时间
      .groupBy(
        F.col("userId"),
        F.window(F.col("orderTime"), "30 minutes", "5 minutes") // 窗口定义
      )
      .count()
      .where("count >= 3")
      .select(
        F.col("userId"),
        F.col("window.end").alias("detectionTime"),
        F.col("count").alias("orderCount")
      )

    // 2. 检测多IP登录（使用状态管理，≥2个不同IP）
    val multiIpEvents = ipLogsStream
      .groupByKey(F.col("userId").cast("string")) // KeyType=String（用户ID）
      .mapGroupsWithState(StateManager.getStateSpec()) { case (userId, ips, state) =>
        state.getOption.flatMap { ipList =>
          if (ipList.size >= 2) Some((userId, ipList)) else None // 过滤≥2个IP的用户
        }
      }
      .select(
        F.col("key").alias("userId"),
        F.col("value").alias("ipAddresses")
      )

    // 3. 关联两类事件并生成预警
    val fraudAlerts = highFrequencyOrders.join(
      multiIpEvents,
      on = F.col("userId"),
      how = "inner"
    ).select(
      F.col("userId"),
      F.col("detectionTime"),
      F.col("orderCount"),
      F.col("ipAddresses"),
      F.concat(
        F.lit("检测到异常：用户"), F.col("userId"),
        F.lit("在"), F.col("detectionTime"),
        F.lit("前30分钟内下单"), F.col("orderCount"),
        F.lit("次，且使用"), F.size(F.col("ipAddresses")).alias("ipCount"),
        F.lit("个不同IP："), F.col("ipAddresses")
      ).alias("alertMessage")
    )

    // 输出到控制台
    fraudAlerts.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }
}
