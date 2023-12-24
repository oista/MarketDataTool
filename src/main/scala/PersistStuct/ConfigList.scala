package PersistStuct

class ConfigList extends ASettingsList {

  // Web-api:
  val acc_token= config.getString("acc_token")
  val eod_uri  = config.getString("end_of_day_uri")
  val ind_uri  = config.getString("intraday_uri")
  val tick_uri = config.getString("tickers_uri")
  val exch_uri = config.getString("exchanges_uri")
 // Postgress:
  val pgs_url  = config.getString("pgs_url")
  val pgs_usr  = config.getString("pgs_usr")
  val pgs_pas  = config.getString("pgs_pas")
  val pgs_sch  = config.getString("pgs_sch")
  val pgs_driver = config.getString("pgs_driver")
  val adapter  = config.getString("db_adapter")

  //Kafka:
  val kfk_in_server = config.getString("kfk_input.server")
  val kfk_In_topic  = config.getString("kfk_input.topic")
  val kfk_out_server= config.getString("kfk_output.server")
  val kfk_out_topic = config.getString("kfk_output.topic")

  //----------------------------------------------------
  override val ListName = "Config list"
  override val NameList = List("acc_token","eod_uri", "ind_uri","tick_uri","exch_uri","adapter")
  override val ValList  = List(
    new StringListObj(acc_token),new StringListObj(eod_uri), new StringListObj(ind_uri), new StringListObj(tick_uri), new StringListObj(exch_uri),
    new StringListObj(pgs_url),  new StringListObj(pgs_usr), new StringListObj(pgs_pas), new StringListObj(pgs_sch),  new StringListObj(pgs_driver),
    new StringListObj(adapter),  new StringListObj(kfk_in_server), new StringListObj(kfk_In_topic), new StringListObj(kfk_out_topic),new StringListObj(kfk_out_topic)
  )

  override val SetList  = (NameList,ValList).zipped.toList


}
