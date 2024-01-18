package ExternalWorker

import PersistStuct.{DataTransformer, SettingStorage}
import DataStructures.{DataEoD, DataStruct, StockData, StreamStruct}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{ StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory


class KafkaLoader {
  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  logger.info(s"Create KafkaLoader")
  val plist = SettingStorage.param_list
  val clist = SettingStorage.conf_list

  implicit val spark = SparkSession.builder() // create Session-factory for spark
    .appName("KafkaSpark")
    .config("spark.master", "local[2]")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

// -------------------------------------------------------------------------------------

  def writeDFtoTopic(sdata:DataFrame,topicName:String )= {
    logger.info(s"KafkaLoader.loadDFToTopic: $topicName start")
    sdata.write
    .format("kafka")
    .option("kafka.bootstrap.servers", clist.kfk_out_server)
    .option("topic", topicName)
    .save()
    logger.info(s"KafkaLoader.writeDFToTopic: write to topic $topicName: finish")
  }

  def readTopicToDF(topicname:String)= {
    logger.info(s"KafkaLoader.readTopicToDF: start reading topic $topicname")

    import org.apache.spark.sql.catalyst.ScalaReflection
    val schemaEOD = ScalaReflection.schemaFor[DataEoD].dataType.asInstanceOf[StructType]
    schemaEOD.printTreeString()

    val kafkaDF = spark.read
      .format("kafka")
      .option("kafkaConsumer.pollTimeoutMs", "20000")
      .option("startingOffsets", "earliest")
      .option("kafka.bootstrap.servers", clist.kfk_in_server)
      .option("subscribe", topicname)
     // .option(ConsumerConfig.GROUP_ID_CONFIG, "Arbitrager")
    //  .option(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)").as[String]
      .select(from_json(col("value"),  schemaEOD).as("jsonDF"))
      .select(col("jsonDF.*"))
      .toDF()

   // logger.info(s"KafkaLoader.readTopicToDF: ")
  //  kafkaDF.show(5)

    logger.info(s"KafkaLoader.readTopicToDF: finish reading topic $topicname - ${kafkaDF.count()} rows")
    kafkaDF
  }

  def readTopicToCaseClass(topicname:String) = {
    val df = readTopicToDF(topicname)
    val ss = DataTransformer.DFtoStreamStruct(df)
    ss
  }
}
