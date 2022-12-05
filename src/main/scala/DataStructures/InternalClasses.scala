package DataStructures

case class PriceMessage(
                       date:      String,
                       ticker:    String,
                       exchange:  String,
                       prc_open:  String,
                       prc_high:  String,
                       prc_low:   String)
object PriceMessage {
  def apply(a: Array[String]): PriceMessage =
    PriceMessage(
      a(0),
      a(1),
      a(2),
      a(3),//.toDouble,
      a(4),//.toDouble,
      a(5)//.toDouble
    )}

case class Condition (
                       date:  String,
                       ticker:    String,
                       exchange:  String
                     )

case class ArbitrageMessage(
         date:            String,
         ticker:          String,
         price_diff_srcs: String,
         price_diff_vals: String,
         price_diff_prc:  String,
         batch_id:        String)
object  ArbitrageMessage {
  def apply(a: Array[String]): ArbitrageMessage =
    ArbitrageMessage(
      a(0),
      a(1),
      a(2),
      a(3),
      a(4),
      a(5)
    )}


case class PriceTable (
                        date: String,
                        symbol: String,
                        exchange: String,
                        open: String,
                        high: String,
                        low: String,
                        close: String,
                        last : String,
                        volume : String)