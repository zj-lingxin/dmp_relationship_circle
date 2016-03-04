package com.asto.dmp.relcir.dao

import java.sql.{DriverManager, ResultSet}
import com.asto.dmp.relcir.base.Props
import com.asto.dmp.relcir.dao.PartyRelDao._
import scala.collection.mutable._

class PartyRelDao {
  def getFromToRel = {
    val conn = DriverManager.getConnection(jdbcConn)
    val result: ArrayBuffer[(String, String)] = new ArrayBuffer()
    try {
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery( """select from_party_uuid,to_party_uuid from party_rel  a where a.rel_type in ("SECOND_CONTACTOR","FIRST_CONTACTOR","THREE_CONTACTOR") and a.to_party_uuid is not null and del_flag = 0""")
      while (rs.next) {
        result += ((rs.getString("from_party_uuid"), rs.getString("to_party_uuid")))
      }
    } finally {
      conn.close
    }
    result.toList
  }

  def partyRelResultInsert(partyRelGroupData: Array[(Long, String, Int)], partyRelFromToData: Array[(Long, String, String)]) = {
    val conn = DriverManager.getConnection(jdbcConn)
    try {
      conn.setAutoCommit(false)
      var prep = conn.prepareStatement("delete from party_rel_group")
      prep.executeUpdate()
      prep = conn.prepareStatement("INSERT INTO party_rel_group(group_id,from_and_to_uuid,role) VALUES (?, ?, ?) ")
      partyRelGroupData.foreach {
        data =>
          prep.setLong(1, data._1)
          prep.setString(2, data._2)
          prep.setInt(3, data._3)
          prep.executeUpdate()
      }

      prep = conn.prepareStatement("delete from party_rel_from_to")
      prep.executeUpdate()
      prep = conn.prepareStatement("INSERT INTO party_rel_from_to(group_id,from_party_uuid,to_party_uuid) VALUES (?, ?, ?) ")
      partyRelFromToData.foreach {
        data =>
          prep.setLong(1, data._1)
          prep.setString(2, data._2)
          prep.setString(3, data._3)
          prep.executeUpdate()
      }
      conn.commit()
    } catch {
      case t: Throwable => conn.rollback()
    } finally {
      conn.close
    }
  }
}

object PartyRelDao {
  val jdbcConn = Props.get("jdbc_conn")
}