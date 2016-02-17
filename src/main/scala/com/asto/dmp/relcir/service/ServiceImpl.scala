package com.asto.dmp.relcir.service

import com.asto.dmp.relcir.base.{Constants, Contexts}
import com.asto.dmp.relcir.dao.PartyRelDao
import com.asto.dmp.relcir.dataframe.{DataQuery, SQL}
import com.asto.dmp.relcir.util.FileUtils
import org.apache.spark.rdd.RDD
import scala.collection.mutable._
import com.asto.dmp.relcir.service.ServiceImpl._

object ServiceImpl {
  val applyCode = 1 //申请人
  val relativeCode = 2 //联系人
  val applyAndRelativeCode = 3 //申请人和联系人
}

class ServiceImpl extends Service {
  val partyRelDao = new PartyRelDao()

/*  val fromToRelRDD = DataQuery.getPartyRelProps(SQL().select("from_party_uuid,to_party_uuid").where("to_party_uuid != 'null'"))
    .map(a => (a(0).toString, a(1).toString))
  val fromToRelList = fromToRelRDD.collect().toList*/

  val fromToRelList = partyRelDao.getFromToRel
  val fromToRelRDD = Contexts.sparkContext.makeRDD(fromToRelList)

  def getTempGroupIds = {
    var index = 0L
    //步骤1： 取 party_rel 表中的 from_party_uuid，并且去除重复项。
    fromToRelList.map(_._1).distinct.map {
      t =>
        index += 1L
        (t, index) //2、给去重后的每个 from_party_uuid 编号为 id。
    }
  }

  def getGroupIds = {
    val tempGroupIds = getTempGroupIds

    var arrayBuffer = ArrayBuffer[(String, Long)]()

    //步骤3：取 party_rel 表中的 from_party_uuid,to_party_uuid 两列，然后和第二步的数据合并，目的是得到对应 to_party_uuid 的 id。
    tempGroupIds.foreach {
      case (key, id) =>
        fromToRelList.filter(t => t._1 == key).foreach { t => arrayBuffer.+=((t._2, id)) }
    }

    //步骤4：取上面得到的 to_party_uuid， id 两列，然后和第二步的 from_party_uuid,id 合并，并且去除重复项，列名取为 from_id,id。这个数据作为下面迭代的输入数据
    arrayBuffer ++= tempGroupIds

    var list = arrayBuffer.toList.map(t => (t._1, (t._2, t._2)))
    val ids = list.map(t => t._2._2).distinct
    var needChangedIds = scala.collection.mutable.ArrayBuffer[Long]()
    ids.foreach {
      id =>
        if (list.map(t => t._2._2).distinct.contains(id)) {
          do {
            val keys = list.filter(t => t._2._2 == id).map(t => t._1).distinct
            needChangedIds.clear()
            list.foreach { t => if (keys.contains(t._1) && t._2._2 != id) needChangedIds += t._2._1 }
            list = list.map(t => if (needChangedIds.contains(t._2._1)) (t._1, (t._2._1, id)) else (t._1, t._2))
          } while (needChangedIds.nonEmpty)
        }
    }

    list.map(t => (t._1, t._2._2)).distinct
  }

  def getRoles = {
    var fromUUIDs = fromToRelList.map(_._1).distinct
    var toUUIDs = fromToRelList.map(t => t._2).distinct
    val bothFromAndToUUIDs = fromUUIDs.intersect(toUUIDs).distinct
    fromUUIDs = fromUUIDs.filter(x => !bothFromAndToUUIDs.contains(x))
    toUUIDs = toUUIDs.filter(x => !bothFromAndToUUIDs.contains(x))
    val fromUUIDsRoles = fromUUIDs.map((_, applyCode))
    val toUUIDsRoles = toUUIDs.map((_, relativeCode))
    val bothFromAndToUUIDsRoles = bothFromAndToUUIDs.map((_, applyAndRelativeCode))
    fromUUIDsRoles.union(toUUIDsRoles).union(bothFromAndToUUIDsRoles)
  }

  def getGroupIdAndRolesRDD(groupIdList: List[(String, Long)], rolesList: List[(String, Int)]) = {
    val groupIdRDD = Contexts.sparkContext.makeRDD(groupIdList)
    val rolesListRDD = Contexts.sparkContext.makeRDD(rolesList)
    groupIdRDD.leftOuterJoin(rolesListRDD).map(t => (t._2._1, t._1, t._2._2.get)).persist()
  }

  def getGroupIdFromUUIDsAndToUUIDsRDD(groupIdAndRolesRDD: RDD[(Long, String, Int)]) = {
    val fromUUIDsAndGroupIdRDD = groupIdAndRolesRDD.filter(t => t._3 == applyCode || t._3 == applyAndRelativeCode) //List((1,a,申请人和联系人), (1,A,申请人), (4,U,申请人), (1,b,申请人和联系人))
      .map(t => (t._2, t._1))
    fromUUIDsAndGroupIdRDD.leftOuterJoin(fromToRelRDD).map(t => (t._2._1, t._1, t._2._2.get)).distinct()
  }

  override protected def runServices(): Unit = {

    val groupIdAndRolesRDD = getGroupIdAndRolesRDD(getGroupIds, getRoles).sortBy(a => a._1)
    val groupIdFromUUIDsAndToUUIDsRDD = getGroupIdFromUUIDsAndToUUIDsRDD(groupIdAndRolesRDD).sortBy(_._1)

    FileUtils.saveAsTextFile(groupIdAndRolesRDD, Constants.OutputPath.PARTY_REL_GROUP)
    FileUtils.saveAsTextFile(groupIdFromUUIDsAndToUUIDsRDD, Constants.OutputPath.PARTY_REL_FROM_TO)

    partyRelDao.partyRelResultInsert(groupIdAndRolesRDD.collect(), groupIdFromUUIDsAndToUUIDsRDD.collect())

  }
}
