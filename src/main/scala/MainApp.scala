

import ExternalWorker.{KafkaLoader, KafkaWorker, PSGLoader, PSGWorker}
import PersistStuct.{Coordinator, DataTransformer}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object MainApp extends {

  /* TO DO:
  - PGS: add params to queries (SQL-module)
  - Change specification from StreamStuct to DataStruct (?)
  - PGS: add table-module
  - KFK: add table-module
  - DataTranformer: add table/streams config
  - Controller: add sys to sys configs
  - PGS: Delete rows before insert (think there is will be appropriate)

   */

  val logger = Logger(LoggerFactory.getLogger(this.getClass))

   def main(args: Array[String]) : Unit = {
     val logger = Logger(LoggerFactory.getLogger(this.getClass))
     logger.info(s"MainApp: start MainApp")
     // web:
   //  val controller = new Coordinator()
  //   val sdata = controller.LoadData
  //   controller.SaveData(sdata)

     // pgs:
     val pgsw = new PSGWorker()
     val cdata = pgsw.produceStructData()
     val cdf = DataTransformer.StructToDF(cdata)
     cdf.show(10)
     logger.info(s"MainApp: PGSWorker read data from DB FINISH")

     // kafka post:
     val kfkw = new KafkaWorker
     kfkw.consumeStructData(cdata)
     logger.info(s"MainApp: KafkaWorker produce data FINISH")

     // kafka get:
     val kdata =  DataTransformer.StructToDF(kfkw.produceStructData())
     kdata.show(10)
     logger.info(s"MainApp: KafkaWorker consume data FINISH")

   }

}
