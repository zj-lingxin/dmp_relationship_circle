package com.asto.dmp.relcir.mq


import scala.util.parsing.json.{JSONArray, JSONObject}

object PartyRelWrapper {
  def getJsonStr(msgList: List[PartyRelMsg]): String = {
    new JSONObject(Map(
      "partyRelList" -> JSONArray(for (msg <- msgList) yield toJsonObj(msg))
    )).toString()
  }

  def toJsonObj(msg: PartyRelMsg): JSONObject = {
    new JSONObject(Map[String, Any]("partyRelGroupId" -> msg.partyRelGroupId, "fromPartyUuid" -> msg.fromPartyUuid, "toPartyUuid" -> msg.toPartyUuid))
  }
}

case class PartyRelMsg(val partyRelGroupId: String, val fromPartyUuid: String, val toPartyUuid: String)



