
import java.io.File
import java.net.URI

import scalaj.http.{Http, HttpOptions, HttpResponse}
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.typesafe.scalalogging._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SaveMode, SparkSession}

import scala.io.Source
import DataStructures.{ExchangeData, IntradayData, Pagination, StockData, TickersData}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import java.time._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

import PGWriter.config
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory


object MainDataLoader  {


  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  val ilist = new InitList
  implicit val  spark = SparkSession.builder()
    .appName("LoadMarketdata")
    .config("spark.master", "local[2]")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  // ------------------------------------------------------------------------------------------------------------------
  class InitList {
    val config = ConfigFactory.load()
    val brokers = config.getString("input.bootstrap.servers")
    val acc_token = config.getString("account_tocken")
    val eod_uri = config.getString("end_of_day_uri")
    val ind_uri = config.getString("intraday_uri")
    val tick_uri = config.getString("tickers_uri")
    val exch_uri = config.getString("exchanges_uri")

    val interval = config.getString("ind_interval")
    val data_type = config.getString("data_type")
    val adapter = config.getString("db_adapter")
    val ticket_list = config.getString("tickers_list").split(",").toList
    val exchange_list = config.getString("exchanges_list").split(",").toList
    val dates_list = config.getString("tradedate_list").split(",").toList
    val first_dt = dates_list.head
    val last_dt = dates_list.last
    val f_dicts = config.getBoolean("F_UPD_DICTS")
    var pg_table = ""

    private val hdfs_conf = new Configuration()
    val HDFS_jsonPath = config.getString("hdfs_json_path") + data_type + "/"
    val HDFS_parquetPath = config.getString("hdfs_json_path") + data_type + "/"
    val fs = FileSystem.get(new URI(config.getString("hdfs_base")), hdfs_conf)

    // temp
    val url = config.getString("pg_url")
  }

  class paginator(){
    val page_size = 1000
    var offset = 0
    val max_page_amount = 9999
    var total_amount = BigInt(0)
    var page_amount = BigInt(1)
    var current_page = BigInt(0)

    def inc_next_page: Unit = {
      current_page = current_page+1
      offset = offset + page_size
      logger.info(s"Gurrent page set to $current_page")
    }


    def set_total_amount(row_amount :BigInt): Unit = {
      total_amount = row_amount
      page_amount = row_amount/page_size
      logger.info(s"Total amount of rows in request set to $total_amount")
      logger.info(s"Page amount set to $page_amount rows")
    }

  }

  def printParams: Unit = {
    logger.info(s"request data_type: $ilist.data_type")
    logger.info(s"request dates: from $ilist.first_dt to $ilist.last_dt")
    logger.info(s"request secs: {}", ilist.ticket_list)
    logger.info(s"request exchanges: {}", ilist.exchange_list)
    logger.info(s"DB adapter: $ilist.adapter")
  }

  def main(args: Array[String]): Unit = {
    printParams
    if (ilist.f_dicts) { updateDicts }

    var pages = new paginator
    ilist.ticket_list.foreach( c_ticket =>
      ilist.exchange_list.foreach(c_exchange => {
       for (p <- 1 to pages.max_page_amount) {
        if (pages.current_page<=pages.max_page_amount) {
         var js_block = ""
         ilist.data_type match {
           case "EOD" => js_block = getEoDRequest(ilist.eod_uri + "?limit=" + pages.page_size + "&offset=" + pages.offset, c_ticket, c_exchange, ilist.first_dt, ilist.last_dt) // End of day data
           case "intraday" => js_block = getIntradayRequest(ilist.ind_uri, c_ticket, c_exchange, ilist.first_dt, ilist.last_dt) // Intraday data
           case "exchanges" => js_block = getExchangeRequest(ilist.exch_uri)} // Data about excanges}


         ilist.adapter match {
           case "hadoop" => {
             var c_path = ilist.HDFS_jsonPath + c_exchange + "/" + c_ticket
             saveStringToHadoop(c_path, parse_json_header_ind(js_block))
           }
           case "postgres" => {
             var tbl_name = ""
             ilist.data_type match {
               case "EOD" => tbl_name = "m_data.t_source_marketdata" // End of day data
               case "intraday" => tbl_name = "m_data.t" // Intraday data
               case "exchanges" => tbl_name = "m_data.dim_exchanges"
             } // Data about excanges
             savetoPostgres(parse_json_header_eod(js_block, pages), tbl_name)
           }
         }
          pages.inc_next_page
       }}
       }))

  }

  def updateDicts(): Unit = {
    var js_block = ""
    val page   = 1000
    var offset = 0

    for (p <- 1 to 10) {
       js_block = getTickerRequest(ilist.tick_uri+"?limit="+page+"&offset="+offset)
      ilist.adapter match {
        case "postgres" => {
          savetoPostgres(parse_json_header_ticker(js_block), "m_data.dim_securities")
        }
      }
      offset = offset + page
    }
  }


  def getIntradayRequest (req_string :String, p_ticket:String, p_exchange:String,  p_start_dt:String,  p_end_dt:String ): String ={
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    // переписать под новые даты
//    var cal:Calendar=Calendar.getInstance()
//        cal.setTime(format.parse(p_date))
//        cal.add(Calendar.DATE,1)
//    val e_date  = format.format(cal.getTime)

    val response: HttpResponse[String] = Http(req_string).params(Map
        ( "access_key"-> ilist.acc_token,
          "symbols"   -> p_ticket,
          "exchange"  -> p_exchange,
          "date_from" -> p_start_dt,
          "date_to"   -> p_end_dt,
          "interval"  -> ilist.interval
           )).asString
    val body = parse_json_header_ind(response.body)
    body.show()
    response.body
  }
  def getEoDRequest      (req_string :String, p_ticket:String, p_exchange:String, p_start_dt:String,  p_end_dt:String): String ={
    val response: HttpResponse[String] = Http(req_string).params(Map
    ( "access_key"-> ilist.acc_token,
      "symbols"   -> p_ticket,
      "exchange"  -> p_exchange,
      "date_from" -> p_start_dt,
      "date_to"   -> p_end_dt,
    )).asString
    response.body
  }
  def getExchangeRequest (req_string :String): String ={
  val response: HttpResponse[String] = Http(req_string).params(Map
  ( "access_key"-> ilist.acc_token
  )).asString
  response.body
}
  def getTickerRequest (req_string :String): String ={
    val response: HttpResponse[String] = Http(req_string).params(Map
    ( "access_key"-> ilist.acc_token
    )).asString
    response.body
  }

  def parse_json_header_eod(js_body: String, pages: => paginator): DataFrame ={
    implicit val eod_codec: JsonValueCodec[StockData] = JsonCodecMaker.make
    val js = readFromArray(js_body.getBytes("UTF-8"))
    if (pages.page_amount==0) {
      pages.set_total_amount(js.pagination.total)}
    js.data.toDF()
  }
  def parse_json_header_ind(js_body: String): DataFrame ={
    implicit val ind_codec: JsonValueCodec[IntradayData] = JsonCodecMaker.make
    val js = readFromArray(js_body.getBytes("UTF-8"))
    val df = js.data.toDF()
    df
  }
  def parse_json_header_exchange(js_body: String): Dataset[Row] ={
    implicit val ind_codec: JsonValueCodec[ExchangeData] = JsonCodecMaker.make
    val js = readFromArray(js_body.getBytes("UTF-8"))
    val df = js.data.toDF()
    val df_res = df.withColumn( "timezone", col("timezone").cast(StringType))
                   .withColumn( "currency", col("currency").cast(StringType))
    df_res.show()
    df_res
  }
  def parse_json_header_ticker(js_body: String): Dataset[Row] ={
    implicit val ind_codec: JsonValueCodec[TickersData] = JsonCodecMaker.make
    val js = readFromArray(js_body.getBytes("UTF-8"))
    val df = js.data.toDF().withColumn("stock_exchange", col("stock_exchange").cast(StringType))
   // df.show()
    println(df.count())
    df
  }


  def convertJson2Parquet(cpath: String): Unit = {
    spark.read.json(cpath).as[StockData].show()
  }



// Save data ---------------------------------------------------------------------

  // 1. Hadoop
  def saveStringToHadoop( dpath: String, cdata: String) {
      println(dpath)
      FileUtils.deleteQuietly(new File(dpath))
      val hwriter = ilist.fs.create(new Path(dpath))
      hwriter.write(cdata.getBytes())
      hwriter.hflush()
      hwriter.close() }

  def saveStringToHadoop( dpath: String, cdata: Dataset[Row]) {
    println(dpath)
    cdata.write.mode(SaveMode.Overwrite).format("parquet").save(dpath)
    }

  def DeleteHDPfolder(dirpath: String): Unit = {
    if (ilist.fs.exists(new Path(dirpath)))
      ilist.fs.delete(new Path(dirpath))
  }

  // 2. Postgres
  def savetoPostgres( cdata: Dataset[Row], v_tbl_name : String): Unit ={
    val rows = cdata.count()

    logger.info(s"try to write $rows rows in $v_tbl_name");
    cdata.write.format("jdbc")
      .option("url", ilist.url)
      .option("user", "m_user")
      .option("password", "m_user")
      .option("dbtable", v_tbl_name)
      .option("stringtype" ,"unspecified")
      .mode("append")
      .save()

    logger.info("pg load complete")
  }

}
