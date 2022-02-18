package DataStructures

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
adj_open: String,//Double,
adj_high: String, //Double,
adj_low: Double,
adj_close: Double,
adj_volume: Double )

// End-of_day data
case class DataEoD(
open: Double,
high: Double,
low: Double,
close: Double,
volume: Double,
adj_high:   Option[String],
adj_low:    Option[String],
adj_close:  Option[Double],
adj_open:   Option[String],
adj_volume: Option[String],
split_factor: Double,
dividend: Double,
symbol: String,
exchange: String,
date: String)

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
)

case class StockData(pagination: Pagination, data: Seq[DataEoD])
case class IntradayData(pagination: Pagination, data: Seq[Intraday])


case class Currency (
code:   Option[String],
symbol: Option[String],
name:   Option[String])

case class Timezone (
timezone: Option[String],
abbr:     Option[String])

case class Exchange (
name:         Option[String],
acronym:      Option[String],
mic:          Option[String],
country:      Option[String],
country_code: Option[String],
city:         Option[String],
website:      Option[String],
timezone:     Option[Timezone],
currency:     Option[Currency]
                    )

//case class ExchangData(pagination: Pagination, data: Seq[Exchange])
case class ExchangData(pagination: Pagination, data: Seq[Exchange])

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
//object  ExchTest {
//  def apply(a: Array[String]): ExchTest =
//    ExchTest(
//      a(0),
//      a(1),
//      a(2),
//      a(3),
//      a(4),
//      a(5)
//    )}

