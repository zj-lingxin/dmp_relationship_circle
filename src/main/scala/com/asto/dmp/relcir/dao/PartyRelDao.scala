package com.asto.dmp.relcir.dao

import java.sql.{DriverManager, ResultSet}
import com.asto.dmp.relcir.base.Props
import com.asto.dmp.relcir.dao.PartyRelDao._
import scala.collection.mutable._

class PartyRelDao {
  def getGroupId(party_uuids: Array[String]): Long = {
    val conn = DriverManager.getConnection(jdbcConn)
    var groupId: Long = 0
    var findId = false
    try {
      val ids = party_uuids.map(id => s""""${id}"""").mkString(",")
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      var rs = statement.executeQuery( s"""select distinct party_rel_group_id from party_rel_group  g where g.party_uuid in ($ids)""")
      while (rs.next && !findId) {
        findId = true
        groupId = rs.getLong("party_rel_group_id")
      }

      //如果在数据库中没有找到相应的groupId，那么就取数据库中最大的groupId + 1
      if (!findId) {
        rs = statement.executeQuery( """ select max(x.party_rel_group_id) maxGroupId from party_rel_group x """)
        groupId = rs.getLong(1) + 1
      }

    } finally {
      conn.close
    }
    groupId
  }

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

  def partyRelResultInsert(partyRelGroupList: Array[(Long, String, String)], partyRelGroup: Array[(Long, String, Int)]) = {
    val conn = DriverManager.getConnection(jdbcConn)

    try {
      conn.setAutoCommit(false)
      var prep = conn.prepareStatement("delete from party_rel_group_list")
      prep.executeUpdate()
      prep = conn.prepareStatement("INSERT INTO party_rel_group_list(party_rel_group_id,from_party_uuid,to_party_uuid) VALUES (?, ?, ?) ")
      partyRelGroupList.foreach {
        data =>
          prep.setLong(1, data._1)
          prep.setString(2, data._2)
          prep.setString(3, data._3)
          prep.executeUpdate()
      }

      prep = conn.prepareStatement("delete from party_rel_group")
      prep.executeUpdate()
      prep = conn.prepareStatement("INSERT INTO party_rel_group(party_rel_group_id,party_uuid,role) VALUES (?, ?, ?) ")
      partyRelGroup.foreach {
        data =>
          prep.setLong(1, data._1)
          prep.setString(2, data._2)
          prep.setInt(3, data._3)
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