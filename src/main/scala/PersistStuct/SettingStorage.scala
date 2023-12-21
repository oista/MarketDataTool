package PersistStuct

import DataStructures.TradeParams

object SettingStorage {
  val conf_list  = new ConfigList()
  val param_list = new ParamList()

  conf_list.printParams()
  param_list.printParams()

  def getConfList:  ConfigList = {conf_list}
  def getParamList: ParamList  = {param_list}
  def getTradeList: List[TradeParams] = {param_list.trParams}

  def getSParamByName(pname:String): String = {param_list.getStringValueByName(pname)}
}
