import DataStructures.{ArbitrageMessage, PriceMessage}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.DoubleType

object PGWriter extends App {
  val config                 = ConfigFactory.load()
  val BootstrapServers       = config.getString("input.bootstrap.servers")
  val checkpointLocation     = config.getString("checkpointLocation")
  val checkpointLocationPG     = config.getString("checkpointLocationPG")
  val src_topic              = config.getString("result_topic")

  // Postgress:
  val url  = config.getString("pg_url")
  val user = config.getString("pg_user")
  val pw   = config.getString("pg_pass")

  val jdbcWriter = new PostgreSqlSink(url,user,pw)

  val spark = SparkSession.builder
    .appName("PGWriter")
    .config("spark.master", "local[2]")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  // ---------------------------------------------------------------------------------------------------------------------

  val arbitrage_data = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",  BootstrapServers)
    .option("subscribe", src_topic)
    .option("failOnDataLoss", false)
      .option(ConsumerConfig.GROUP_ID_CONFIG, "Postgress")
    .option(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .load()
    .withWatermark("timestamp", "2 hours")
    .selectExpr("CAST(value AS STRING)")
    .as[String]
    .map(_.split(","))
    .map(ArbitrageMessage(_))
    .as[ArbitrageMessage]
    .toDF()

  //write to postgress
  val pgs_stream = arbitrage_data
    .writeStream
    .foreach(jdbcWriter)
    .outputMode("Append")
    .trigger(ProcessingTime("5 seconds"))
    .option("checkpointLocation", checkpointLocationPG)
    .start()

  // write to console
  val con_stream = arbitrage_data
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

  spark.streams.awaitAnyTermination()
  // ---------------------------------------------------------------------------------------------------------------------


  class PostgreSqlSink(url: String, user: String, pwd: String)
    extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
    val driver = "org.postgresql.Driver"
    var connection: java.sql.Connection = _
    var statement: java.sql.PreparedStatement = _
    val v_sql = s"insert INTO T_IND_ONLINE_ARBITRAGE (date, ticker, source_code, price_difference, price_prc_difference, batch_id) values(?, ?, ?, ? , ?, ?)"

    def open(partitionId: Long, version: Long): Boolean = {
      Class.forName(driver)
      connection = java.sql.DriverManager.getConnection(url, user, pwd)
      connection.setAutoCommit(false)
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
