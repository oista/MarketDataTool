package ExternalWorker

import DataStructures.StreamStruct
import PersistStuct.{DataTransformer, SettingStorage}


class KafkaWorker extends ExtConsumer with ExtProducer {

  val clist = SettingStorage.getConfList
  val plist = SettingStorage.getParamList
  logger.info(s"PSGLoader: connection for user $clist.pgs_usr");


  override def consumeStructData(sdata: Seq[StreamStruct]): String = {
   val kfk_loader = new KafkaLoader
   val src_df = DataTransformer.StructToDF(sdata.toList);
   val kfk_df = DataTransformer.mergeDFforKafka(src_df)

    kfk_loader.writeDFtoTopic(kfk_df , clist.kfk_out_topic)
    logger.info(s"KafkaWorker: consume StructData complete");
    "OK"
  }

  override def produceStructData(): Seq[StreamStruct] = {
    val kfk_loader = new KafkaLoader
    val res_df = kfk_loader.readTopicToDF(clist.kfk_In_topic)
    val res_ds = DataTransformer.DFtoStreamStruct(res_df)
    res_ds
  }
}
