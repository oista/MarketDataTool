package PersistStuct

import DataStructures.{DataEoD, DataStruct, StockData, StreamStruct}
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._


object DataTransformer {

  System.setProperty("hadoop.home.dir", "C:\\winutil\\")
  implicit val spark = SparkSession.builder()
    .appName("DataTransformer")
    .config("spark.master", "local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val conf_list  = new ConfigList()
  val param_list = new ParamList()

  // Stream to Data : -----------------------------------------------

  def StructToDF(sSeq: Seq[StreamStruct]) : DataFrame = {
    SdataToDF(StructToData(sSeq), param_list.data_type)}

  def StructToData(sSeq: Seq[StreamStruct]) : Seq[Seq[DataStruct]] = {
    for (p <- sSeq) yield {
      p.sdata}}

  def SdataToDF(s: Seq[Seq[DataStruct]], stream_type: String): DataFrame = {
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

  //  Data to Stream: ------------------------------
  def DataStructToStreamStruct(stream: Seq[DataStruct]) : Seq[StreamStruct] = {
    for (p <- stream) yield {
      new StockData(null,stream.asInstanceOf[List[DataEoD]])
    }
  }

  def DFtoStreamStruct(src_df: DataFrame) = {
    val resDS = DFtoDataStruct(src_df)
    val resSS = DataStructToStreamStruct(resDS)
    resSS
  }
  def DFtoDataStruct(src_df:DataFrame) = {
    val resDF = src_df.as[DataEoD]
    val resDS = resDF.map(r => DataEoD(r.open, r.high, r.low, r.close, r.volume,
      r.adj_high, r.adj_low, r.adj_close, r.adj_open, r.adj_volume, r.split_factor,
      r.dividend, r.symbol, r.exchange, r.date)).collect().toSeq
    resDS
  }

  def mergeDFforKafka(src_df: DataFrame): DataFrame = {
    var res_df: DataFrame = null
    param_list.data_type match {

      case "EOD" => res_df =
        src_df
          .withColumn("key",lit("1"))
          .withColumn("value", to_json(struct("open", "high", "low",
          "close", "volume", "adj_high", "adj_low", "adj_close", "adj_open", "adj_volume",
          "split_factor", "dividend", "symbol", "exchange","date")))
          .select("key", "value")

    }
    res_df
  }
}

