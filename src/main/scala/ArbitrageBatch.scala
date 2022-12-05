import DataStructures.{PriceMessage, PriceTable}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, lit, max, min, sum}
import org.apache.spark.sql.types.DoubleType

object ArbitrageBatch extends App {

  val config    = ConfigFactory.load()
  val HDFS_jsonPath     =  config.getString("hdfs_json_path")+"intraday/"
  val ticket_list    = config.getString("tickers_list").split(",").toList
  val exchange_list  = config.getString("exchanges_list").split(",").toList
  val dates_list     = config.getString("tradedate_list").split(",").toList

  val url  = config.getString("pg_url")
  val user = config.getString("pg_user")
  val pw   = config.getString("pg_pass")
  val tab_batch  = config.getString("t_batch_total")

  implicit val  spark = SparkSession.builder()
    .appName("KafkaProducer")
    .config("spark.master", "local[2]")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  // ------------------------------------------------------------------------------------------------------------------

    load_day_total_to_db
  //  show_day_arbitrage("IEXG","XNYS")

  // ------------------------------------------------------------------------------------------------------------------

  def load_day_total_to_db(): Unit ={
    var df_marketdata = Seq.empty[PriceTable].toDF
    exchange_list.foreach(exchange =>
      dates_list.foreach(cdate => {
        ticket_list.foreach(cticket => {
          val cpath = HDFS_jsonPath + exchange + "/" + cdate + "/" + exchange + "_" + cticket
          println("HDFS path:" + cpath)
          val df_mdpart = spark.read.parquet(cpath)
          df_marketdata = df_marketdata.unionAll(df_mdpart)
        })
        //write day data
        val df_result = df_marketdata
          .groupBy("exchange", "symbol")
          .agg(
            min("date").as("period_start"),
            max("date").as("period_end"),
            avg("open").cast(DoubleType).as("avg_price"),
            max("high").cast(DoubleType).as("top_price"),
            min("low").cast(DoubleType) as ("low_price"),
          )
          .withColumn("period_type", lit("day"))

        df_result.write
          .format("jdbc")
          .mode(SaveMode.Append)
          .option("url", url)
          .option("user",user)
          .option("password", pw)
          .option("dbtable", tab_batch)
          .option("driver", "org.postgresql.Driver")
          .save()
      }
      ))
  }

  def show_day_arbitrage(exchange1:String,exchange2:String): Unit ={

    val sql = "select   t1.exchange, t1.symbol, t1.period_start, t1.period_end,  " +
              " (t1.avg_price-t2.avg_price) as price_diff, (t1.top_price-t2.top_price) as top_diff, " +
              " (t1.low_price-t2.low_price) as low_diff from " +
             $" (select * from T_IND_BATCH_PERIOD t where t.period_type='day' and t.exchange='$exchange1') t1" +
              " inner join  " +
              $"(select * from T_IND_BATCH_PERIOD t  where t.period_type='day' and t.exchange='$exchange2') t2 " +
              "on      " +
              "t1.exchange=t2.exchange and t1.period_start=t2.period_start and t1.period_end=t2.period_end and t1.symbol=t2.symbol "

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", pw)
      .option("query", sql)
      .load()
    jdbcDF.createOrReplaceTempView("source")

    spark.sql("select * from source ").show()
  }
}

