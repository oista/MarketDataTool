package WebApi

import DataStructures.StreamStruct
import PersistStuct.{ SettingStorage}

trait webapi {

  def getRequest  : String
  def parseResponeHeader(cbody : Respone) :  StreamStruct
}

class api_data{
  val plist  = SettingStorage.getConfList
  val api_token:String = plist.getStringValueByName("acc_token")
  val url_eod = plist.getStringValueByName("eod_uri")
  val url_ind = plist.getStringValueByName("ind_uri")
  val url_exc = plist.getStringValueByName("exch_uri")
}