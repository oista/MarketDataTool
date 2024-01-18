package DataStructures
import org.apache.spark.sql.DataFrame

case class Pagination(
limit:  BigInt,
offset: BigInt,
count:  BigInt,
total:  BigInt
 )

// Market Indices
case class DataINDX(
date: String,
symbol: String,
exchange: String,
open: Double,
high: Double,
low: Double,
close: Double,
volume: Double,
adj_open: Double,
adj_high: Double,
adj_low: Double,
adj_close: Double,
adj_volume: Double ) extends DataStruct

// End-of_day data
case class DataEoD  (
open: Double,
high: Double,
low: Double,
close: Double,
volume: Double,
adj_high:   Option[Double],
adj_low:    Option[Double],
adj_close:  Option[Double],
adj_open:   Option[Double],
adj_volume: Option[Double],
split_factor: Double,
dividend: Double,
symbol: String,
exchange: String,
date: String) extends DataStruct

object  DataEoD {
  def apply(open: Double, high: Double, low: Double, close: Double, volume: Double,
            adj_high: Option[Double], adj_low: Option[Double], adj_close: Option[Double],
            adj_open: Option[Double], adj_volume: Option[Double], split_factor: Double,
            dividend: Double, symbol: String, exchange: String, date:String): DataEoD =
    DataEoD(open, high, low, close, volume, adj_high, adj_low, adj_close,
      adj_open, adj_volume, split_factor, dividend, symbol, exchange, date)}


case class Intraday (
date: String,
symbol: String,
exchange: String,
open: Option[Double],
high: Option[Double],
low: Option[Double],
close: Option[Double],
last: Option[Double],
volume: Option[Double]
) extends DataStruct


case class Currency (
code:   Option[String],
symbol: Option[String],
name:   Option[String])

case class Timezone (
timezone: Option[String],
abbr:     Option[String],
abbr_dst: Option[String]                    )

case class Exchange (
name:         Option[String],
acronym:      Option[String],
mic:          Option[String],
country:      Option[String],
country_code: Option[String],
city:         Option[String],
website:      Option[String],
timezone:     Option[Timezone],
currency:     Option[Currency]) extends DataStruct

case class Ticker(
  name:   Option[String],
  symbol: Option[String],
  stock_exchange : Option[Exchange]) extends DataStruct

case class StockData(pagination: Pagination, data: Seq[DataEoD]) extends StreamStruct {
val sdata = data
}

case class IntradayData(pagination: Pagination, data: Seq[Intraday]) extends StreamStruct{
 val sdata = data
}
case class ExchangeData(pagination: Pagination, data: Seq[Exchange]) extends StreamStruct{
  val sdata = data
}
case class TickersData(pagination: Pagination, data: Seq[Ticker]) extends StreamStruct{
  val sdata = data}


case class ExchTest(
      acronym:      Option[String],
      city:         Option[String],
      country:      Option[String],
      country_code: Option[String],
      currency:     Option[Currency],
      mic:          Option[String],
      name:         Option[String],
      timezone:     Option[Timezone],
      website:      Option[String]
                    )


