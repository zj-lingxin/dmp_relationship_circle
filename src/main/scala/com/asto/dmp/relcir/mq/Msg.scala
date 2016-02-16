package com.asto.dmp.relcir.mq

import scala.util.parsing.json.{JSONArray, JSONObject}

object MsgWrapper {
  def getJson(majorBusiness: String, msgList: List[Msg]): String = {
    new JSONObject(Map(
      "majorBusiness" -> majorBusiness,
      "quotaItemList" -> JSONArray(for (msg <- msgList) yield toJsonObj(msg))
    )).toString()
  }

  def getJson(majorBusiness: String, msgs: Msg *): String = {
    getJson(majorBusiness: String, msgs.toList)
  }

  def toJsonObj(msg: Msg): JSONObject = {
    new JSONObject(Map[String, Any]("indexFlag" -> msg.indexFlag, "quotaCode" -> msg.quotaCode,"quotaValue" -> msg.quotaValue, "targetTime" -> msg.targetTime, "quotaName" -> msg.quotaName))
  }
}

class Msg(val quotaCode: String, val quotaValue: Any, val targetTime: String , val indexFlag: String = "2", val quotaName: String = "")


