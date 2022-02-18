
import java.io.File
import java.net.URI

import scalaj.http.{Http, HttpOptions, HttpResponse}
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SaveMode, SparkSession}

import scala.io.Source
import DataStructures.{IntradayData, Pagination, StockData}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import java.time._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

import scala.collection.BitSet.empty.to

object MainDataLoader  {

  val config    = ConfigFactory.load()
  val brokers   = config.getString("input.bootstrap.servers")
  val acc_token = config.getString("account_tocken")
  val eod_uri   = config.getString("end_of_day_uri")
  val ind_uri   = config.getString("intraday_uri")
  val tick_uri  = config.getString("tickers_uri")

  var exchange  = config.getString("exchange")

  val interval       = config.getString("ind_interval")
  val data_type      = config.getString("data_type")
  val ticket_list    = config.getString("tickers_list").split(",").toList
  val exchange_list  = config.getString("exchanges_list").split(",").toList
  val dates_list     = config.getString("tradedate_list").split(",").toList

  private val hdfs_conf = new Configuration()
  val HDFS_jsonPath     = config.getString("hdfs_json_path")+data_type+"/"
  val HDFS_parquetPath  = config.getString("hdfs_json_path")+data_type+"/"
  val fs = FileSystem.get(new URI(config.getString("hdfs_base")), hdfs_conf)

  implicit val  spark = SparkSession.builder()
    .appName("LoadMarketdata")
    .config("spark.master", "local[2]")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  // ------------------------------------------------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {

   // DeleteHDPfolder("hdfs://127.0.0.1:9000//marketdata/")

    dates_list.foreach(cdate =>
      ticket_list.foreach( cticket =>
       {
         var js_block = ""
         data_type match {
           case "EOD"      => js_block = getEoDRequest(eod_uri, cticket, exchange, cdate)       // End of day data
           case "intraday" => js_block = getIntradayRequest(ind_uri, cticket, exchange, cdate)  // Intraday data
           case "tickers"  => js_block = getTickersRequest(tick_uri)  // Data about excange tickers
         }
         var c_path = HDFS_jsonPath+exchange+"/"+cdate+"/"+exchange+"_"+cticket
         saveJSToHadoop(c_path, parse_json_header_ind2(js_block))
       }))


  }


  def getIntradayRequest (req_string :String, p_ticket:String, p_exchange:String, p_date:String ): String ={
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
        cal.setTime(format.parse(p_date))
        cal.add(Calendar.DATE,1)
    val e_date  = format.format(cal.getTime)

    val response: HttpResponse[String] = Http(req_string).params(Map
        ( "access_key"-> acc_token,
          "symbols"   -> p_ticket,
          "exchange"  -> p_exchange,
          "date_from" -> p_date,
          "date_to"   -> e_date,
          "interval"  -> interval
           )).asString
    val body = parse_json_header_ind(response.body)
    body.show()
    response.body
  }
  def getEoDRequest      (req_string :String, p_ticket:String, p_exchange:String, p_date:String): String ={
    val response: HttpResponse[String] = Http(req_string).params(Map
    ( "access_key"-> acc_token,
      "symbols"   -> p_ticket,
      "exchange"  -> p_exchange,
      "date_from" -> p_date,
      "date_to"   -> p_date,
    )).asString
    val body = parse_json_header_eod(response.body)
    body.show()
    response.body
  }
  def getTickersRequest  (req_string :String): String ={
    val response: HttpResponse[String] = Http(req_string).params(Map
    ( "access_key"-> acc_token,
      "exchange"  -> exchange
    )).asString
    response.body
  }


  def parse_json_header_eod(js_body: String): DataFrame ={
    implicit val eod_codec: JsonValueCodec[StockData] = JsonCodecMaker.make
    val js = readFromArray(js_body.getBytes("UTF-8"))
    js.data.toDF()
  }
  def parse_json_header_ind(js_body: String): DataFrame ={
    implicit val ind_codec: JsonValueCodec[IntradayData] = JsonCodecMaker.make
    val js = readFromArray(js_body.getBytes("UTF-8"))
    js.data.toDF().show()
    js.data.toDF()
  }
  def parse_json_header_ind2(js_body: String): Dataset[Row] ={
    implicit val ind_codec: JsonValueCodec[IntradayData] = JsonCodecMaker.make
    val js = readFromArray(js_body.getBytes("UTF-8"))

    val test = js.data.toDF()
    test.show()
    test
  }

  def parse_json(js_body: String)  {

    implicit val codec: JsonValueCodec[StockData] = JsonCodecMaker.make

    val mdata = readFromArray(js_body.getBytes("UTF-8"))


  }

  def convertJson2Parquet(cpath: String): Unit = {
    spark.read.json(cpath).as[StockData].show()
  }

  def GetINDRequestString(x_exchange:String, x_ticker:String, x_date:String): String = {
"rgrhg"
  }

// Save data ---------------------------------------------------------------------
 // def saveDFToHadoop(df: Dataset[Row], fpath:String): Unit ={
  //  df.write.mode(SaveMode.Overwrite).format("parquet").save(fpath)}

  def saveJSToHadoop( dpath: String, cdata: String) {
      println(dpath)
      FileUtils.deleteQuietly(new File(dpath))
      val hwriter = fs.create(new Path(dpath))
      hwriter.write(cdata.getBytes())
      hwriter.hflush()
      hwriter.close() }

  def saveJSToHadoop( dpath: String, cdata: Dataset[Row]) {
    println(dpath)
    cdata.write.mode(SaveMode.Overwrite).format("parquet").save(dpath)
    }

  def DeleteHDPfolder(dirpath: String): Unit = {
    if (fs.exists(new Path(dirpath)))
      fs.delete(new Path(dirpath))
  }


}
