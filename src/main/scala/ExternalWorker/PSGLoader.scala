package ExternalWorker

import DataStructures.{DataEoD, DataStruct}
import PersistStuct.{DataTransformer, SettingStorage}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import java.sql.ResultSet

class PSGLoader(val table_name:String) {
  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  logger.info(s"Create PSGWorker for $schema.$table_name table")
  val plist = SettingStorage.param_list
  val clist = SettingStorage.conf_list
  val schema = clist.pgs_sch
  val connection = java.sql.DriverManager.getConnection(PG_Connect.pgs_url, PG_Connect.pgs_usr, PG_Connect.pgs_pas)
  connection.setAutoCommit(true)

  def saveDF(cdata: DataFrame) = {
    val rows = cdata.count()
    logger.info(s"PSGLoader: start write $rows rows to $schema.$table_name");
    cdata.show()
    cdata.write.format("jdbc")
      .option("url",     PG_Connect.pgs_url)
      .option("user",    PG_Connect.pgs_usr)
      .option("password",PG_Connect.pgs_pas)
      .option("dbtable", schema+"."+table_name)
   //   .option("stringtype", "unspecified")
      .mode("append")
      .save()
    logger.info("PSGLoader: load complete")
  }

  def loadDF() = {
    val dstruct = loadDataStruct()
    logger.info(s"PSGLoader.loadDF: list size is ${dstruct.size}")
    DataTransformer.SdataObjToDF(dstruct,plist.data_type)
  }

  def loadDataStruct()={
    logger.info(s"PSGLoader.loadDataStruct: start load for $schema.$table_name table")

    val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    val rs = statement.executeQuery(s"select * from $schema.$table_name")
    var rs_seq : List[DataStruct] = Nil
    while (rs.next) {
    val n = DataEoD(rs.getDouble("open"), rs.getDouble("high"), rs.getDouble("low"), rs.getDouble("close"), rs.getDouble("volume"),
        Some(rs.getDouble("adj_high")), Some(rs.getDouble("adj_low")), Some(rs.getDouble("adj_close")), Some(rs.getDouble("adj_open")), Some(rs.getDouble("adj_volume")),
        rs.getDouble("split_factor"), rs.getDouble("dividend"), rs.getString("symbol"), rs.getString("exchange"), rs.getString("date"))
      rs_seq = rs_seq.::(n)}
    rs_seq
  }


  class PostgreSink(tbl_name: String, tbl_col: Int)
    extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
//    var connection: java.sql.Connection = _
    var statement: java.sql.PreparedStatement = _
    var v_sql = ""

    // move to metastorage!:
    tbl_name match {
      case "t_source_marketdata" => v_sql = s"insert t_source_marketdata (price_open ,price_high ,price_low ,price_close, volume ,adj_high, adj_low, adj_close,adj_open, adj_volume, split_factor, dividend ,symbol ,exchange ,date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    }

    def open(partitionId: Long, version: Long): Boolean = {
//      connection = java.sql.DriverManager.getConnection(PG_Connect.pgs_url,PG_Connect.pgs_usr, PG_Connect.pgs_pas)
//      connection.setAutoCommit(false)
      statement = connection.prepareStatement(v_sql)
      true
    }

    def process(value: org.apache.spark.sql.Row): Unit = {
      statement.setString(1, value(0).toString)
      statement.setString(2, value(1).toString)
      statement.setString(3, value(2).toString)
      statement.setString(4, value(3).toString)
      statement.setString(5, value(4).toString)
      statement.setString(6, value(5).toString)
      statement.executeUpdate()
    }

    def close(errorOrNull: Throwable): Unit = {
      connection.commit()
      connection.close
    }
  }

}
