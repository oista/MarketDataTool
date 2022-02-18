
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{array, col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import scala.util.Random

object MainProducer extends App {

  val exchange = {args(0)}


  val config    = ConfigFactory.load()
  val HDFS_jsonPath     =  config.getString("hdfs_json_path")+"intraday/"
  val ticket_list    = config.getString("tickers_list").split(",").toList
  val exchange_list  = config.getString("exchanges_list").split(",").toList
  val dates_list     = config.getString("tradedate_list").split(",").toList


  implicit val  spark = SparkSession.builder()
    .appName("KafkaProducer")
    .config("spark.master", "local[2]")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")


  // -----------------------------------------------------------------------------------------------------------------
  println("start producer. Exchange-"+exchange)

  dates_list.foreach(cdate =>
    ticket_list.foreach( cticket => {
      val cpath = HDFS_jsonPath + exchange + "/"+cdate+"/"+exchange+"_"+cticket
      println("HDFS path:" + cpath)
      send_file_to_kafka(cpath)}
    ))


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
  }

}
