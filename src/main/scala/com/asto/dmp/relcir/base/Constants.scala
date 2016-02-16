package com.asto.dmp.relcir.base

object Constants {

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
  /** 输入文件路径 **/
  object InputPath {
    val SEPARATOR = "\t"

    private val ONLINE_DIR = s"${App.DIR}/input/online/${App.TODAY}/${App.TIMESTAMP}"
    val PARTY_REL_FROM_TO = s"$ONLINE_DIR/party_rel_from_to"
  }
  

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val ONLINE_DIR = s"${App.DIR}/output/online/${App.TODAY}/${App.TIMESTAMP}"
    val RESULT_GROUP = s"$ONLINE_DIR/result_group"
    val RESULT_SUM = s"$ONLINE_DIR/result_sum"
    val RESULT_REL = s"$ONLINE_DIR/result_rel"
  }

  /** 表的模式 **/
  object Schema {
    val PARTY_REL_FROM_TO = "from_party_uuid,to_party_uuid"
  }
}
