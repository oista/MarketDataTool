package Services

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

class Calendar {

  var dT = Calendar.getInstance()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")


  def getCurrentDate:String= {java.time.LocalDate.now.toString}

  def getDateMinusN(cdate: String, n: Int):String = {
    val runDay = LocalDate.parse(cdate, formatter)
    val runDayM= runDay.minusDays(n)
    runDayM.toString
  }
}
