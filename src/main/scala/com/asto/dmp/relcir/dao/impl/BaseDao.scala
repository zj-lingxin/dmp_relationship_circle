package com.asto.dmp.relcir.dao.impl

import com.asto.dmp.relcir.base._
import com.asto.dmp.relcir.dao.{Dao, SQL}

object BaseDao extends Dao {
  def getPartyRelProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.PARTY_REL_FROM_TO , Constants.Schema.PARTY_REL_FROM_TO , "PARTY_REL_FROM_TO", sql)
}
