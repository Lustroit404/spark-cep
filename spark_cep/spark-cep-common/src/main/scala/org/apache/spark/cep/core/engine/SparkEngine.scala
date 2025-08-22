package org.apache.spark.cep.core.engine
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.cep.core.model._
import org.apache.spark.cep.core.rules._
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.{SPARK_BRANCH, streaming}
import org.apache.spark.cep.core.engine.traits._


class SparkEngine(val spark:SparkSession,
                  val accumulate:Accumulate=new SparkAccumulate(),
                  val process: Process=new SparkProcess(),
                  val multiProcess: MultiProcess=new SparkMultiProcess()) {
  def executeSingleRule(rule:Either[WindowRule,StateRule]):DataFrame={
    rule match {
      case Left(windowRule)=>
        val accumulated=accumulate.run(spark, source = windowRule.source, format = windowRule.format, timeGranularity = windowRule.slideIntervalMs)
        process.run(spark,Seq(accumulated),windowRule.condition)

      case Right(stateRule)=>
        val accumulated=accumulate.run(spark, source = stateRule.source, format = stateRule.format, timeGranularity = stateRule.stateTimeoutMs)
        process.run(spark,Seq(accumulated),stateRule.condition)
    }
  }

  def executeCompositeRule(compositeRule: CompositeRule):DataFrame={
    val subRuleResults=compositeRule.rules.flatMap{
      subRule=>executeSingleRule(subRule) match{
        case df if !df.isEmpty=>
          Some(df)
        case _ => None
      }
    }
    multiProcess.run(spark,subRuleResults,compositeRule.joinCondition)
  }


}
