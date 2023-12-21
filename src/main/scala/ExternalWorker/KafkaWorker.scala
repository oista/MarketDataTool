package ExternalWorker

import DataStructures.StreamStruct
import com.typesafe.scalalogging._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, lit, struct}
import org.apache.spark.sql.types.StringType

import scala.util.Random


class KafkaWorker extends ExtWorker with App { // ExtWorker

  implicit val spark = SparkSession.builder()
    .appName("KafkaProducer")
    .config("spark.master", "local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  //val logger = Logger(LoggerFactory.getLogger(this.getClass))

 // override def SendData(cdata:StreamStruct ,source:  ExternalSystem,target:  ExternalSystem) {}

//  def send_file_to_kafka(p_fpath: String, trParams: TradeParams) {
//    // fix api intraday data
//    val ind_data = spark.read.parquet(p_fpath)
//    val rdy_data = ind_data.withColumn("exchange", lit(trParams.exchange))
//      .withColumn("open", $"open" + Random.nextFloat())
//
//    val val_data = rdy_data.withColumn("value_trans",
//      array($"date",
//        $"symbol",
//        $"exchange",
//        $"open",
//        $"high",
//        $"low"))
//    val send_data = val_data.withColumn("value", col("value_trans").cast(StringType))
//    send_data.select("value").show()
//
//    send_data.write
//      .format("kafka")
//     // .option("kafka.bootstrap.servers", config.getString("input.bootstrap.servers"))
//      .option("topic", trParams.exchange)
//      .save()

   // logger.info(s"query sent to kafka topic - : $exchange")
  //}


}
