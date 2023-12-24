package PersistStuct

import DataStructures.{StreamStruct}
import ExternalWorker.ExtFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


class Coordinator () {
  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  val factory = new ExtFactory
  val producer = factory.get_producer()
  val consumer = factory.get_consumer()


def LoadData: Seq[StreamStruct] = {
  val pr_data = producer.produceStructData()
  logger.info("Coordinator.LoadData: loaded data is...")
  DataTransformer.StructToDF(pr_data).show()
  logger.info("Coordinator.LoadData: successfully DONE")
  pr_data
}

  def SaveData(sdata : Seq[StreamStruct]) = {
    consumer.consumeStructData(sdata)
    logger.info("Coordinator.SaveData: successfully DONE")
  }



}
