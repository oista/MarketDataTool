
import DataStructures.{ExchTest, ExchangData, Exchange, IntradayData, PriceMessage}
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromArray}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils.trim
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.functions.{avg, col, concat_ws, explode, from_json, lit, when}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SaveMode, SparkSession}

import scala.Console.println


  object ArbitrageProcessing extends App {

    val config                 = ConfigFactory.load()
    val BootstrapServers       = config.getString("input.bootstrap.servers")
    val checkpointLocation     = config.getString("checkpointLocation")
    val Topics                 = "XNAS,IEXG"//config.getString("output.topic")
    val res_topic              = config.getString("result_topic")
    val exch_list              = Seq(" IEXG"," XNAS")
    val exch_data              = config.getString("exchange_data")
    val rate_data              = config.getString("rate_data")


    val spark = SparkSession.builder
      .appName("ArbitrageApp")
      .config("spark.master", "local[2]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // ----------------------------------------------------------------------------------------------------------------
println("start")
    try {

    val input_data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",  BootstrapServers)
      .option("subscribe", Topics)
      .option("failOnDataLoss", false)
      .option(ConsumerConfig.GROUP_ID_CONFIG, "Arbitrager")
      .option(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .load()
      .withWatermark("timestamp", "2 hours")
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.split(","))
      .map(PriceMessage(_))
      .as[PriceMessage]
      .withColumn( "prc_open",  col("prc_open").cast(DoubleType))
      .withColumn( "avg_price", col("prc_open").cast(DoubleType))
      .toDF()
      .as("src")

      // Exchange dictionary:
      val exch_stream = spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .load(exch_data).toDF().as("exch")

      // Currency rate:
      val rate_stream = spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .load(rate_data).toDF().as("rate")


   val join_data = input_data
     .join(exch_stream , $"src.exchange"  === $"exch.mic"     ,"left_outer")
     .join(rate_stream , $"exch.currency" === $"rate.currency","left_outer")
     .withColumn("prc_open", $"prc_open"*$"rate")


//--------------------------------------------------------------------------------------------------------------------

    val arb_stream = join_data
      .writeStream
      .trigger(ProcessingTime("5 seconds"))
      .option("checkpointLocation", checkpointLocation )
      .option("truncate", "false")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val b_source = batchDF
          // pivot and get price difference:
          .groupBy( "date", "ticker")
          .pivot("exchange", exch_list)
          .avg("prc_open")
          .withColumn("price_diff_vals", col(exch_list(0))-col(exch_list(1)) )
          .withColumn("price_diff_prc",   (col(exch_list(0))-col(exch_list(1))) /
                                                    ((col(exch_list(0))+col(exch_list(1)))/2)
                      )
          .withColumn("price_diff_srcs", lit(trim(exch_list(0))+"_"+trim(exch_list(1)) ))
          .withColumn("batch_id", lit(batchId) )
          .select($"date", $"ticker", $"price_diff_srcs", $"price_diff_vals", $"price_diff_prc", $"batch_id")
          .filter("price_diff_vals<>0 and price_diff_vals is not null")

         val b_kafka = b_source
           .withColumn("key", $"batch_id".cast(StringType) )
           .withColumn("value",
             concat_ws(",",$"date", $"ticker", $"price_diff_srcs",
                                       $"price_diff_vals", $"price_diff_prc", $"batch_id") )
           .select($"key", $"value")
           .write
           .format("kafka")
           .option("kafka.bootstrap.servers", BootstrapServers)
           .option("topic", res_topic)
           .mode(SaveMode.Append)
           .save()

         val b_console = b_source
          .write
          .format("console")
          .mode(SaveMode.Append)
          .save()
      }

      .start()
      .awaitTermination()

      // spark.streams.awaitAnyTermination()
    }
    catch {
      case e: Exception =>
        println(s"ERROR: ${e.getMessage}")
        sys.exit(-1)
    } finally {
      spark.stop()
    }


    }