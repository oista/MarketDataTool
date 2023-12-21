package DataStructures


abstract class StreamStruct {

  val sdata : Seq[DataStruct]

}


abstract class DataStruct {

}


class TradeParams   (val exchange:String,  val ticker:String, val dt_start:String, val dt_end: String, val interval : String)