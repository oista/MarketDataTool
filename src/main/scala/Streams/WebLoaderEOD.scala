package Streams

import DataStructures.{StockData, StreamStruct, TradeParams}
import WebApi.{Request, Respone}
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromArray}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}

class WebLoaderEOD() extends WebLoader {
  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  var request : Request = new Request(new TradeParams("","","","",""))


  override def get_req_data(p_request: Request): StreamStruct= {
    request = p_request
    val rbody:String = getRequest()
    val pbody = parseResponeHeader(new Respone(rbody))
    logger.info(s"WebLoaderEOD: get_req_data DONE ")
    pbody }


  def getRequest(): String = {
    val req_string = request.init_api_data.url_eod + "?limit=10 &offset=0"// request.offset
    val response: HttpResponse[String] = Http(req_string).params(Map
    ("access_key" -> request.init_api_data.api_token,
      "symbols"   -> request.tr_param.ticker,
      "exchange"  -> request.tr_param.exchange,
      "date_from" -> request.tr_param.dt_start,
      "date_to"   -> request.tr_param.dt_end,
    )).asString
    logger.info(s"WebLoaderEOD: respone ready")
    response.body
  }

  def parseResponeHeader(cbody: Respone): StreamStruct = {
    implicit val eod_codec: JsonValueCodec[StockData] = JsonCodecMaker.make
    println(cbody.body)
    val js = readFromArray(cbody.body.getBytes("UTF-8"))
     if (request.current_page == 0) {
       request.set_total_amount(js.pagination.total) }
    logger.info(s"WebLoaderEOD: parse is ready ")
    js
  }


}
