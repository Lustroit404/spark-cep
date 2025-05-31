package org.apache.spark.cep.constants

object CEPConstants {
  val kafka_topic_orders="user_orders"
  val kafka_topic_ips="user_ip_logs"
  val checkpoint_path="/tmp/cep/checkpoint"
  val window_size="30 minutes"
  val slide_interval="5 minutes"

}
