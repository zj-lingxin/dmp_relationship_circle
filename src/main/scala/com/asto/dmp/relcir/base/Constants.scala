package com.asto.dmp.relcir.base

object Constants {
 // private val prop = new Props()
  object App {
    val NAME = "关系圈算法"
    val LOG_WRAPPER = "##########"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    val DIR = s"${Hadoop.DEFAULT_FS}/dmp_relcir"
    var TODAY: String = _
    var TIMESTAMP: Long = _
    val ERROR_LOG: StringBuffer = new StringBuffer("")
    var MESSAGES: StringBuffer = new StringBuffer("")
    var SAVE_MIDDLE_FILES = false
  }
  
  object Hadoop {
    val JOBTRACKER_ADDRESS = "appcluster"
    val DEFAULT_FS = s"hdfs://$JOBTRACKER_ADDRESS"
  }

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val PATH = s"${App.DIR}/output/${App.TODAY}/${App.TIMESTAMP}"
    val PARTY_REL_GROUP = s"$PATH/party_rel_group"
    val PARTY_REL_GROUP_LIST = s"$PATH/party_rel_group_list"
  }


}
