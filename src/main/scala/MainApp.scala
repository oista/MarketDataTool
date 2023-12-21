

import ExternalWorker.PSGLoader
import PersistStuct.Coordinator
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object MainApp extends {

  /* TO DO:
  - Delete rows before insert (think there is will be appropriate)

   */

  val logger = Logger(LoggerFactory.getLogger(this.getClass))

   def main(args: Array[String]) : Unit = {
    // val controller = new Coordinator()
    // val sdata = controller.LoadData
    // controller.SaveData(sdata)

       val pgs = new PSGLoader("t_source_marketdata")
       val pdata = pgs.loadDF()//pgs.loadDataStruct()
     pdata.show()

   }

}
