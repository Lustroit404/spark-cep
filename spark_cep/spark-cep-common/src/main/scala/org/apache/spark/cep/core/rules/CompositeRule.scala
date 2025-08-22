package org.apache.spark.cep.core.rules

case class CompositeRule (
                         name:String,
                         rules:List[Either[WindowRule,StateRule]],
                         joinCondition:String //connection Condition

                         )
