package PersistStuct

import DataStructures.TradeParams
import Services.Calendar


class ParamList  extends ASettingsList {
  val cl = new Calendar()

  val source_system = config.getString("source_system")
  val target_system = config.getString("target_system")
  val stream_type   = config.getString("stream_type")

  // trade params:
  val interval      = config.getString("ind_interval")
  val data_type     = config.getString("data_type")
  val ticket_list   = config.getString("tickers_list").split(",").toList
  val exchange_list = config.getString("exchanges_list").split(",").toList
  val dates_list    = config.getString("tradedate_list").split(",").toList


  // set auto-params for date detection:
  val autodate   = config.getString("autodate")
  val autodate_n = config.getString("autodate_n")

  val last_dt = autodate match {
    case null => dates_list.last
    case _    => cl.getCurrentDate}

  val first_dt = autodate match {
    case null => dates_list.head
    case _    => cl.getDateMinusN(last_dt, autodate_n.toInt)}



  var trParams = for {
    exchange <- exchange_list
    ticker <- ticket_list
  } yield new TradeParams(exchange, ticker, first_dt, last_dt, interval)


  override val ListName = "Stream param list"

  override val NameList: List[String] = List("interval","data_type","ticket_list","exchange_list",
    "dates_list","first_dt","last_dt","trd_params","source_system","target_system","stream_type")

  override val ValList: List[ASetListEllement] =
    List(new StringListObj(interval), new StringListObj(data_type),
    new SeqStringListObj(ticket_list),new SeqStringListObj(exchange_list),
    new SeqStringListObj(dates_list),new StringListObj(first_dt),new StringListObj(last_dt),
      new TradeListObj(trParams), new StringListObj(source_system),new StringListObj(target_system),
      new StringListObj(stream_type)
    )

  override val SetList: List[(String, ASetListEllement)] = (NameList,ValList).zipped.toList

}
