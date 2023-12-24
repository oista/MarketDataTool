package ExternalWorker

import DataStructures.{ StreamStruct}
import PersistStuct.{DataTransformer, SettingStorage}
import com.typesafe.config.ConfigFactory

class PSGWorker extends ExtConsumer with ExtProducer   {
  val clist  = SettingStorage.getConfList
  val plist  = SettingStorage.getParamList
  logger.info(s"PSGLoader: connection for user $clist.pgs_usr");


 def getTableName(stream_type:String):String ={
  stream_type match {
  case "EOD"=>"t_source_marketdata"}}

  override def consumeStructData(sdata: Seq[StreamStruct]) = {
    val df = DataTransformer.StructToDF(sdata)
    val pg_loader = new PSGLoader(getTableName(plist.data_type))
    pg_loader.saveDF(df)
    logger.info(s"PSGLoader: consume StructData complete");
    "OK"}

  override def produceStructData(): Seq[StreamStruct] = {
    val pg_loader = new PSGLoader(getTableName(plist.data_type))
    val sdata = pg_loader.loadDataStruct()
    val res: Seq[StreamStruct] = DataTransformer.StreamStructToDataStruct(sdata)
    logger.info(s"PSGLoader: consume StructData complete");
    res
  }
}

object PG_Connect {
  val config = ConfigFactory.load()
  val pgs_url = config.getString("pgs_url")
  val pgs_usr = config.getString("pgs_usr")
  val pgs_pas = config.getString("pgs_pas")
}
