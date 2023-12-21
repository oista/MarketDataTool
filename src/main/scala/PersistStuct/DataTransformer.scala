package PersistStuct

import DataStructures.{DataEoD, DataStruct, StockData, StreamStruct}
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataTransformer {

  System.setProperty("hadoop.home.dir", "C:\\winutil\\")
  implicit val spark = SparkSession.builder()
    .appName("LoadMarketdata")
    .config("spark.master", "local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val conf_list  = new ConfigList()
  val param_list = new ParamList()


  def StructToDF(sSeq: List[StreamStruct]) = {
    SdataToDF(StructToData(sSeq), param_list.data_type)}

  def StructToData(sSeq: List[StreamStruct]):List[Seq[DataStruct]] = {
    for (p <- sSeq) yield {
      p.sdata}}

  def SdataToDF(s: List[Seq[DataStruct]], stream_type: String): DataFrame = {
    stream_type match {
      case "EOD" => {
        var resDF = Seq.empty[DataEoD].toDF; for (d <- s) {
          resDF = resDF.union(SdataObjToDF(d,stream_type))
        };
        resDF
      }
    }}

  def SdataObjToDF(sobj: Seq[DataStruct], stream_type: String) : DataFrame ={
    stream_type match {
      case "EOD" => {sobj.asInstanceOf[List[DataEoD]].toDF()}
    }
  }


}

