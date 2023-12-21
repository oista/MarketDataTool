package ExternalWorker

import DataStructures.StreamStruct
import PersistStuct.{SettingStorage}
import Streams.{WebLoader, WebLoaderEOD, WebLoaderIND}
import WebApi.Request

class WebApiWorker() extends ExtProducer {
  val plist  = SettingStorage.getParamList

  val WLoader: WebLoader =
    plist.getStringValueByName("data_type") match {
      case "EOD" => new WebLoaderEOD()
      case "IND" => new WebLoaderIND()
      case "EXNG"=> new WebLoaderEOD()
    }
  logger.info(s" Create WebApiWorker for ${plist.getStringValueByName("data_type")} datatype")

  override def produceStructData(): List[StreamStruct] = {
    logger.info(s" WebApiWorker: produce Struct Data")
    for (pr <- plist.trParams) yield {WLoader.get_req_data(new Request(pr))}
    }

  }


