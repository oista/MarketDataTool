
import PG_Connect._
import com.typesafe.scalalogging._
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{array, col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession

import scala.util.Random

object MainProducer extends App {

  val exchange = "XNAS"


  val config    = ConfigFactory.load()
  val HDFS_jsonPath  = config.getString("hdfs_json_path")+"intraday/"
  val ticket_list    = config.getString("tickers_list").split(",").toList
  val exchange_list  = config.getString("exchanges_list").split(",").toList
  val dates_list     = config.getString("tradedate_list").split(",").toList


  implicit val  spark = SparkSession.builder()
    .appName("KafkaProducer")
    .config("spark.master", "local[2]")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")
  val logger = Logger(LoggerFactory.getLogger(this.getClass))


  // -----------------------------------------------------------------------------------------------------------------
  logger.info("start producer. Exchange-"+exchange)
  for (p <- 1 to 10000) {
    dates_list.foreach(cdate =>
      ticket_list.foreach(cticket => {
        //  send_file_to_kafka(cpath)
        send_query_to_kafka("select * from m_data.T_SOURCE_MARKETDATA ")
      }
      ))
  }


  def send_file_to_kafka(p_fpath:String) {
    // fix api intraday data
    val ind_data = spark.read.parquet(p_fpath)
    val rdy_data = ind_data.withColumn("exchange", lit(exchange))
      .withColumn("open", $"open" + Random.nextFloat())

    val val_data = rdy_data.withColumn("value_trans",
      array($"date",
        $"symbol",
        $"exchange",
        $"open",
        $"high",
        $"low"))
    val send_data = val_data.withColumn("value", col("value_trans").cast(StringType))
    send_data.select("value").show()

    send_data.write
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("input.bootstrap.servers"))
      .option("topic", exchange)
      .save()

    logger.info(s"query sent to kafka topic - : $exchange")
  }

  def send_query_to_kafka(p_query:String): Unit ={

    val ind_data = spark.read
      .format("jdbc")
      .option("url", pg_url)
      .option("user",pg_usr)
      .option("password", pg_psw)
      .option("query", p_query)
      .option("driver", "org.postgresql.Driver")
      .load()

    val val_data = ind_data.withColumn("value_trans",
      array($"date",
        $"symbol",
        $"exchange",
        $"open",
        $"high",
        $"low",
        $"close"))
    val send_data = val_data.withColumn("value", col("value_trans").cast(StringType))
    send_data.select("value").show()

  //  ind_data.createOrReplaceTempView("source")
   // spark.sql("select * from source ").show()

    send_data.write
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("input.bootstrap.servers"))
      .option("topic", exchange)
      .save()

  }
}
