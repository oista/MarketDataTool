package PersistStuct

import DataStructures.{DataEoD, DataStruct, StockData, StreamStruct}
import org.apache.spark.sql.functions.{ col, struct}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

import scala.collection.immutable.List


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

  def StructToDF(sSeq: Seq[StreamStruct]) = {
    SdataToDF(StructToData(sSeq), param_list.data_type)}

  def StructToData(sSeq: Seq[StreamStruct]):Seq[Seq[DataStruct]] = {
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
  def StreamStructToDataStruct(stream: Seq[DataStruct]) : Seq[StreamStruct] = {
    for (p <- stream) yield {
      new StockData(null,stream.asInstanceOf[List[DataEoD]])
    }
  }

  // ------------------------------------------------
  def mergeColumns(row: Row) = {
    val column1 = row.getAs[String]("column1")
    val column2 = row.getAs[String]("column2")
    (column1, column2)
  }

  def mergeDFforKafka(src_df:DataFrame):DataFrame = {
    var res_df: DataFrame = null
    param_list.data_type match {

      case "EOD" => res_df = src_df.withColumn("value",
        struct(col("open"),
          col("high"),
          col("low"),
          col("close"),
          col("volume"),
          col("adj_high"),
          col("adj_low"),
          col("adj_close"),
          col("adj_open"),
          col("adj_volume"),
          col("split_factor"),
          col("dividend"),
          col("symbol"),
          col("exchange"),
          col("date")).cast(StringType)
      )
    }
    res_df
  }

  def DFtoDataStruct(src_df:DataFrame) = {
    val cEncoder = Encoders.bean(DataEoD.getClass)
    val resDF = src_df.as[DataEoD](cEncoder)
    resDF
  }
}

