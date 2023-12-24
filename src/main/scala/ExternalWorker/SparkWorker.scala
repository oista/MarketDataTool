package ExternalWorker

import org.apache.spark.sql.SparkSession

class SparkWorker   {
  implicit val spark = SparkSession.builder()
    .appName("KafkaProducer")
    .config("spark.master", "local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
}
