package com.asto.dmp.relcir.dataframe

import com.asto.dmp.relcir.base._

object DataQuery extends DataFrame {
  def getPartyRelProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.PARTY_REL_FROM_TO , Constants.Schema.PARTY_REL_FROM_TO , "PARTY_REL_FROM_TO", sql)
}
