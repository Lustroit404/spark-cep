package org.apache.spark.cep.core.RuleParser

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.cep.core.rules._
import org.apache.spark.sql.functions._
class JSONRuleParser(spark:SparkSession) extends BaseRuleParser {
  import spark.implicits._
  override def ParseFromFile(path: String): CompositeRule = {
    val content=scala.io.Source.fromFile(path).mkString
    ParseFromString(content)
  }

  override def ParseFromString(content: String): CompositeRule = {

    val JsonDF=spark
      .read.schema(jsonSchema).json(Seq(content).toDS())
    val baseInfo:Row=JsonDF.select(col("cep.rules.compositeRule.name").alias("name"),col("cep.compositeRule.joinCondition").alias("joinCondition")
    ).head()
    val name=baseInfo.getAs[String]("name")
    val joinCondition=baseInfo.getAs[String]("joinCondition")
    /***
     * parsing window rules(***)
     * */
    val windowRules=JsonDF.select(explode(col("cep.rules.compositeRule.windowRules")).alias("rule"))
      .select(
        col("rule.windowSizeMs").cast(LongType),
        col("rule.slideIntervalMs").cast(LongType),
        col("rule.watermarkDelayMs").cast(LongType),
        col("rule.groupByKey"),
        col("rule.eventTimeField"),
        col("rule.condition"),
        col("rule.resultFields"),
        col("rule.source"),
        col("rule.format")
      )
      .as[(Long,Long,Long,String,String,String,Seq[String],String,String)]
      .map{case(ws,si,wd,gk,et,cond,rf,sc,fm)=>
        Left(WindowRule(ws,si,wd,gk,et,cond,rf.toList,sc,fm))
      }.collect()
      .toList

    val stateRules=JsonDF.select(explode(col("cep.rules.compositeRule.stateRules")).alias("rule"))
      .select(
        col("rules.groupByKey"),
        col("rule.stateType"),
        col("rule.source"),
        col("rule.format"),
        col("rule.stateSizeLimit").cast(IntegerType),
        col("rule.condition"),
        col("rule.stateTimeoutMs").cast(LongType),

      )
      .as[(String,String,String,String,Int,String,Long)]
      .map{case(gk,st,sc,fmt,ssl,cond,stm)=>
        Right(StateRule(gk,st,sc,fmt,ssl,cond,stm))
      }
      .collect()
      .toList

    CompositeRule(name,windowRules++stateRules,joinCondition)



  }
  private val jsonSchema=StructType(Seq(StructField("cep",StructType(Seq(StructField("name",StringType),StructField("joinCondition",StringType),
    StructField("windowRules",ArrayType(StructType(Seq(
      StructField("windowSizeMs",LongType),
      StructField("slideIntervalMs",LongType),
      StructField("watermarkDelayMs",LongType),
      StructField("groupByKey",LongType),
      StructField("eventTimeField",StringType),
      StructField("condition",StringType),
      StructField("resultFields",LongType)
    )))),
    StructField("stateRules",ArrayType(StructType(Seq(
      StructField("groupByKey",StringType),
      StructField("stateType",StringType),
      StructField("stateSizeLimit",IntegerType),
      StructField("condition",StringType),
      StructField("stateTimeField",StringType)
    ))))
  )))))
}
