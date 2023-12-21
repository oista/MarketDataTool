package Streams

import DataStructures.{IntradayData, StockData, StreamStruct, TradeParams}
import PersistStuct.SettingStorage
import WebApi.{Request, Respone}
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromArray}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}

class WebLoaderIND ()   extends WebLoader {
  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  var request : Request = new Request(new TradeParams("","","","",""))

  override def get_req_data(p_req: Request): StreamStruct = {
    request = p_req
    val rbody: String = getRequest
    val pbody = parseResponeHeader(new Respone(rbody))
    pbody
  }

  def getRequest: String = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val req_string = request.init_api_data.url_ind + "?limit=" +request.page_size + "&offset=" + request.offset
    val plist = SettingStorage.getParamList

    val response: HttpResponse[String] = Http(req_string).params(Map
    ("access_key" -> request.init_api_data.api_token,
      "symbols"   -> plist.getStringValueByName("ticker"),
      "exchange"  -> plist.getStringValueByName("exchange"),
      "date_from" -> plist.getStringValueByName("dt_start"),
      "date_to"   -> plist.getStringValueByName("dt_end"),
      "interval"  -> plist.getStringValueByName("interval"))).asString
    response.body
  }

  def parseResponeHeader(cbody: Respone): StreamStruct = {
    implicit val ind_codec: JsonValueCodec[IntradayData] = JsonCodecMaker.make
    val js = readFromArray(cbody.body.getBytes("UTF-8"))
    js
  }
}
