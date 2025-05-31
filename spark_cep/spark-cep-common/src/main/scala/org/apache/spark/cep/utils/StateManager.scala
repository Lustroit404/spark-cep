package org.apache.spark.cep.utils

import org.apache.spark.cep.models.IPLog
import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.streaming
import org.apache.spark.sql.{Encoder, Encoders}

object StateManager {
  type IpState=List[String]
  type MappedType=(String,IpState)
  implicit val ipStateEncoder:Encoder[IpState]=Encoders.product[IpState]
  def updateIpState(userId:String, ips:Seq[IPLog], state:State[IpState]):(String,IpState)={
    val currentIps=state.getOption.getOrElse(List.empty[String])
    val newIps=ips.map(_.ipAddress).toList

    val updatedIps=(newIps++currentIps).distinct.take(10)
    state.update(updatedIps)
    (userId,updatedIps)
  }

  def getStateSpec():StateSpec[String,IPLog,IpState,(String,IpState)]={
    StateSpec.function(updateIpState _)
  }
}
