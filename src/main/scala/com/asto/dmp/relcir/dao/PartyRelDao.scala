package com.asto.dmp.relcir.dao

import java.sql.{DriverManager, ResultSet}
import com.asto.dmp.relcir.base.Props
import com.asto.dmp.relcir.dao.PartyRelDao._

class PartyRelDao {
  def basicQuery = {
    val conn = DriverManager.getConnection(jdbcConn)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs = statement.executeQuery("select from_party_uuid,to_party_uuid from party_rel where to_party_uuid is not null limit 15")

      // Iterate Over ResultSet
      while (rs.next) {
        println(rs.getString("from_party_uuid") + " " + rs.getString("to_party_uuid"))
      }
    } finally {
      conn.close
    }
  }

  def partyRelGroupInsert(dataArray: Array[(Long, String, Int)]) = {
    val conn = DriverManager.getConnection(jdbcConn)

    // do database insert
    try {
      conn.setAutoCommit(false)
      var prep = conn.prepareStatement("delete from party_rel_group")
      prep.executeUpdate()
      prep = conn.prepareStatement("INSERT INTO party_rel_group(group_id,from_and_to_uuid,role) VALUES (?, ?, ?) ")
      dataArray.foreach {
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