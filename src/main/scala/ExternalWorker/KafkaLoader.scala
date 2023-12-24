package ExternalWorker

import PersistStuct.{DataTransformer, SettingStorage}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
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
    logger.info(s"KafkaLoader.loadDFToTopic: $topicName finish")
  }

  def readTopicToDF(topicname:String)= {
    logger.info(s"KafkaLoader.readTopicToDF: start reading topic $topicname")
    val kafkaDF = spark.read
      .format("kafka")
      .option("kafkaConsumer.pollTimeoutMs", "20000")
      .option("startingOffsets", "earliest")
      .option("kafka.bootstrap.servers", clist.kfk_in_server)
      .option("subscribe", topicname).load()
    logger.info(s"KafkaLoader.readTopicToDF: start reading topic $topicname - ${kafkaDF.count()}")
    kafkaDF
  }

  def readTopicToCaseClass(topicname:String) = {
    val df = readTopicToDF(topicname)
    DataTransformer.DFtoDataStruct(df)
  }
}
