package CoreTestingExample
import org.apache.spark.sql.{SparkSession, functions}

import org.apache.spark.cep.core.{engine, rules}
import org.apache.spark.cep.core.RuleParser._
import org.apache.spark.cep.core.rules._
import org.apache.spark.cep.core.engine.SparkEngine
object codingExamination {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().master("local[2]").getOrCreate()
    val engine=new SparkEngine(spark)
    val ResultField_1=List("userID","event_time")

    val windowRule_1=WindowRule(windowSizeMs = 300000,
      slideIntervalMs = 200000, watermarkDelayMs = 300000, groupByKey = "userID", eventTimeField = "event", condition = "'action'='click'", resultFields = ResultField_1.toList,
      source = "/mnt/d/systemDir/Desktop/test.csv", format = "csv"
    )
    val stateRule_1=StateRule(groupByKey = "session_id", stateType = "session", source = "/mnt/d/systemDir/Desktop/test.csv", format = "csv", stateSizeLimit = 2, condition = "a.userID=b.userID", stateTimeoutMs = 300000)


    val singleRule_1=CompositeRule(name="Lustroit404_test_composite",rules=List(Left(windowRule_1),Right(stateRule_1)),joinCondition = "window.user_id=state.session_id")
    val result=engine.executeCompositeRule(singleRule_1)
    println(result.count())
    spark.stop
  }
}
