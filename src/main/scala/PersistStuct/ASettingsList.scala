package PersistStuct

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

abstract class ASettingsList {

  val ListName : String = "Undeff list"
  val NameList : List[String]= List(" grg")
  val ValList  : List[ASetListEllement] = List(new StringListObj("-1"))
  val SetList  : List[(String, ASetListEllement)]

  val config = ConfigFactory.load()
  val logger = Logger(LoggerFactory.getLogger(this.getClass))

 // if (NameList.size!=ValList.size) {println("error")}

  def getValueByNumb(pnumb: Int) : Unit = {ValList(pnumb)};
  def getValueByName(pname :String) : Unit = {SetList.find(_._2 == pname).map(_._1)}
  def getStringValueByNumb(pnumb:Int): String ={SetList(pnumb)._2.getStringVal()}
  def getStringValueByName(pname:String): String ={SetList(NameList.indexOf(pname))._2.getStringVal()}

  def getNameByNumb(pnumb: Int) : String = {NameList(pnumb)}
  def getNumbByName(pname: String) : Int= {NameList.indexOf(pname)}

  def getNameList(): List[String] = NameList;
  def getValList() : List[ASetListEllement] = ValList;
  def getList()    : Seq[(String,ASetListEllement)]= SetList;
  def getUnitList(): Seq[Unit] = {for {e <- ValList} yield (e.asInstanceOf[Unit])}


  def printParams()= {
    println(s"Params for $ListName:")
    for (p <- SetList) { println(s"${getNumbByName(p._1.toString)+1}) ${p._1.toString} : ${p._2.getStringVal()}")}
    println(s"----------------------------------------------")

  }

}
