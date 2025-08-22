package org.apache.spark.cep.core.RuleParser
import org.apache.spark.cep.core.rules.CompositeRule
abstract class BaseRuleParser {
  def ParseFromFile(path:String):CompositeRule
  def ParseFromString(content:String):CompositeRule
}
