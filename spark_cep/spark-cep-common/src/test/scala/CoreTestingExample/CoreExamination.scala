package CoreTestingExample
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.cep.core.{engine, rules}
import org.apache.spark.cep.core.RuleParser._
import org.apache.spark.cep.core.engine.SparkEngine
object CoreExamination {
  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().master("local[*]").getOrCreate()
    val ruleParser=new txtRuleParser(spark:SparkSession)
    val compositeRule=ruleParser.ParseFromFile("file://rules.txt")
    val engine=new SparkEngine(spark)
    val result=engine.executeCompositeRule(compositeRule)
    result.show()
  }

}
