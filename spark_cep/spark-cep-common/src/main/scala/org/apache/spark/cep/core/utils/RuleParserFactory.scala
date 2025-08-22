package org.apache.spark.cep.core.utils
import org.apache.spark.cep.core.RuleParser._
import org.apache.spark.cep.core.rules._
import org.apache.spark.sql.{SparkSession,Row,functions}
object RuleParserFactory {
  def getParser(path:String,spark:SparkSession):BaseRuleParser={
    path match{
      case p if p.endsWith("json")=> new JSONRuleParser(spark)
      case p if p.endsWith("txt")=>new txtRuleParser(spark)
      case _ =>throw new IllegalArgumentException(s"Unsupported file format,$path")
    }
  }
}


