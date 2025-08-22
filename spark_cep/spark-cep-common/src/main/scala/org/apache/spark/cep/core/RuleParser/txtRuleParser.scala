package org.apache.spark.cep.core.RuleParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.cep.core.rules._
import org.apache.spark.sql.functions._
class txtRuleParser(spark:SparkSession) extends BaseRuleParser {
  import spark.implicits._

  override def ParseFromFile(path: String): CompositeRule = {
    val content=scala.io.Source.fromFile(path).mkString
    ParseFromString(content)
  }

  override def ParseFromString(content: String): CompositeRule = {
    val lines=content.split("\n")
      .map(_.trim)
      .filter(line =>line.nonEmpty && !line.startsWith("#"))

    val name=lines.find(_.startsWith("name="))
      .map(_.split("=")(1)).getOrElse("defaultRule")

    val joinCondition = lines.find(_.startsWith("joinCondition="))
      .map(_.split("=")(1)).getOrElse("key")

    val windowRule=WindowRule(windowSizeMs = lines.find(_.startsWith("windowRules.windowSizeMs="))
    .map(_.split("=")(1).toLong).getOrElse(1800000L),
      slideIntervalMs = lines.find(_.startsWith("windowRules.slideIntervalMs="))
        .map(_.split("=")(1).toLong).getOrElse(300000L),
      watermarkDelayMs=lines.find(_.startsWith("windowRules.watermarkDelayMs="))
        .map(_.split("=")(1).toLong).getOrElse(1800000L),
      groupByKey=lines.find(_.startsWith("windowRules.groupByKey="))
        .map(_.split("=")(1)).getOrElse("userID"),
      eventTimeField = lines.find(_.startsWith("windowRules.eventTimeField="))
        .map(_.split("=")(1)).getOrElse("eventTime"),
      condition=lines.find(_.startsWith("windowRules.Condition="))
        .map(_.split("=")(1)).getOrElse("count>=3"),
      resultFields=lines.find(_.startsWith("windowRules.resultFields="))
        .map(_.split("=")(1).split(",").toList).getOrElse(List("user_id","default_field")),
      source=lines.find(_.startsWith("windowRules.source="))
        .map(_.split("=")(1)).getOrElse("parquet"),
      format=lines.find(_.startsWith("windowRules.format="))
        .map(_.split("=")(1)).getOrElse("parquet")

    )
    val windowRules=List(Left(windowRule))
    val stateRule=StateRule(
      groupByKey=lines.find(_.startsWith("stateRules.groupByKey="))
        .map(_.split("=")(1)).getOrElse("userID"),
      stateType =lines.find(_.startsWith("stateRules.stateType="))
        .map(_.split("=")(1)).getOrElse("list"),
      source=lines.find(_.startsWith("stateRules.source="))
        .map(_.split("=")(1)).getOrElse("parquet"),
      format=lines.find(_.startsWith("stateRules.format"))
        .map(_.split("=")(1)).getOrElse("parquet"),
      stateSizeLimit = 5,
      condition=lines.find(_.startsWith("stateRules.condition="))
        .map(_.split("=")(1)).getOrElse("size>=2"),
      stateTimeoutMs = lines.find(_.startsWith("stateRules.stateTimeoutMs="))
        .map(_.split("=")(1).toLong).getOrElse(604800000L),

    )
    val stateRules=List(Right(stateRule))

    CompositeRule(name,windowRules++stateRules,joinCondition)
  }
}
