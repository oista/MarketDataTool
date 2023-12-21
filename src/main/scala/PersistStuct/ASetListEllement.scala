package PersistStuct

import DataStructures.TradeParams

trait ASetListEllement {
def getStringVal():String {}
}

class StringListObj(a: String) extends ASetListEllement{
  val ell : String = a
   def apply {ell}
  override def getStringVal():String={ell.toString}
}

class SeqStringListObj(a: List[String]) extends ASetListEllement {
  val ell: List[String] = a
  def apply {ell}
  override def getStringVal(): String = {
    var res: String = ""
    for {e <- ell} {res = res.concat("," + e.toString)}
    res}
}

  class NumbListObj(a: Int) extends ASetListEllement {
    val ell: Int = a
    def apply {ell}
    override def getStringVal(): String = {ell.toString}
  }


  class TradeListObj(a: List[TradeParams]) extends ASetListEllement {
    val ell: List[TradeParams] = a
    def apply {ell}
    override def getStringVal(): String = {
      var res: String = ""
      for {e <- ell} {res = res.concat(s" ${e.dt_start}/${e.dt_end} ${e.ticker} ${e.exchange} ${e.interval}")}
      res}
  }
