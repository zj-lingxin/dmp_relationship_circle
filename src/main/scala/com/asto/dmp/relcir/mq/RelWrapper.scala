package com.asto.dmp.relcir.mq

import scala.util.parsing.json.{JSONArray, JSONObject}

object RelWrapper {
  def getJsonStr(msgList: List[RelMsg]): String = {
    new JSONObject(Map(
      "relList" -> JSONArray(for (msg <- msgList) yield toJsonObj(msg))
    )).toString()
  }

  def toJsonObj(msg: RelMsg): JSONObject = {
    new JSONObject(Map[String, Any]("partyRelGroupId" -> msg.partyRelGroupId, "partyUuid" -> msg.partyUuid, "role" -> msg.role))
  }
}

case class RelMsg(val partyRelGroupId: String, val partyUuid: String, val role: String)



