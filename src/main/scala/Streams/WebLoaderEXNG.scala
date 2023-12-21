package Streams

import DataStructures.{ExchangeData, StreamStruct, TradeParams}
import WebApi.{Request, Respone}
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromArray}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}

class WebLoaderEXNG () extends  WebLoader {
  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  var request : Request = new Request(new TradeParams("","","","",""))

  override def get_req_data(p_request: Request): StreamStruct = {
    request = p_request
    val rbody:String = getRequest
    val pbody = parseResponeHeader(new Respone(rbody))
    logger.info(s"WebLoaderEXNG: respone body ")
    pbody
  }

  def getRequest: String = {
    val req_string = request.init_api_data.url_eod + "?limit="+request.page_size+ "&offset="+ request.offset
    val response: HttpResponse[String] = Http(request.init_api_data.url_exc).params(Map
    ("access_key" -> request.init_api_data.api_token)).asString
    response.body
  }

  def parseResponeHeader(cbody: Respone): StreamStruct = {
    implicit val ind_codec: JsonValueCodec[ExchangeData] = JsonCodecMaker.make
    val js = readFromArray(cbody.body.getBytes("UTF-8"))
    js
  }
}
