package ExternalWorker

import DataStructures.{IntradayData, StockData, StreamStruct}
import PersistStuct.SettingStorage
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.immutable.List


trait ExtWorker {
  val  logger = Logger(LoggerFactory.getLogger(this.getClass))
 def get_transport_type(src: ExtWorker, trg: ExtWorker): String = {
  List(src, trg) match {
   case List(p1: WebApiWorker, p2: PSGWorker) => "BATCH"
   // case List(p1:WebApiWorker, p2:HDPWorker) => "BATCH"
   case List(p1: PSGWorker, p2: KafkaWorker) => "BATCH"
   case List(p1: PSGWorker, p2: SparkWorker) => "BATCH"
   case List(p1: KafkaWorker, p2: SparkWorker) => "STREAM"
  }}

 }

class ExtFactory {
  val  logger = Logger(LoggerFactory.getLogger(this.getClass))
  val plist  = SettingStorage.getParamList

 def get_producer(): ExtProducer = {
   val producer = SettingStorage.getSParamByName("source_system")
    match {
     case "WEBAPI" => new WebApiWorker()}
   logger.info(s" Create producer for ${SettingStorage.getSParamByName("stream_type")} stream (${SettingStorage.getSParamByName("source_system")})")
   producer}

 def get_consumer(): ExtConsumer = {
   val consumer =  SettingStorage.getSParamByName("target_system") match {
    case "POSTGRES" => new PSGWorker}
   logger.info(s" Create consumer for ${SettingStorage.getSParamByName("stream_type")} stream (${SettingStorage.getSParamByName("target_system")})")
 consumer}

def get_system(ps:String): ExtSystem = {
  logger.info(s" Create ExtSystem for $ps")
 new ExtSystem(ps)
}

  }

class ExtSystem(val name:String) {
  val logger = Logger(LoggerFactory.getLogger(this.getClass))
}

trait ExtProducer extends ExtWorker {
 def produceStructData(): Seq[StreamStruct];
}

trait ExtConsumer extends ExtWorker {
  def consumeStructData(sdata: Seq[StreamStruct]):String;
}
