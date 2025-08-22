package CoreTestingExample
import org.apache.spark.sql.{SparkSession, functions,DataFrame}

import org.apache.spark.cep.core.{engine, rules}
import org.apache.spark.cep.core.RuleParser._
import org.apache.spark.cep.core.engine.SparkEngine
object txtRuleParserExamination {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val ruleFilePath="src/test/scala/CoreTestingExample/rules.txt"
    val tempViewName="accumulate_temp"
    val data=Seq((1,"virtual_event",1690000000L))
    val virtualDF=data.toDF("id","event_type","event_time")
    try{
      val parser=new txtRuleParser(spark)
      val compositeRule=parser.ParseFromFile(ruleFilePath)
      println(s"rule parse success, subrule total:${compositeRule.rules.size}")

      val engine=new SparkEngine(spark)
      val actualResult=engine.executeCompositeRule(compositeRule)
      actualResult.show(5)
      println("execute success")

    }catch{
      case e:Exception=>
        println(s"process exception: ${e.getMessage}")
        e.printStackTrace()
    }finally{
      spark.stop()
    }
  }
}
