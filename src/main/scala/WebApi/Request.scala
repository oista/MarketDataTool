package WebApi


import DataStructures.TradeParams
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


class Request (p_tr_param : TradeParams){
  val logger = Logger(LoggerFactory.getLogger(this.getClass))

  val page_size = 1000
  var offset = 0
  val max_page_amount = 9999
  var total_amount = BigInt(0)
  var page_amount  = BigInt(1)
  var current_page = BigInt(0)
  var tr_param = p_tr_param

  val init_api_data: api_data = new api_data()

  def inc_next_page: Unit = {
    if (current_page <= page_amount) {
      current_page = current_page + 1
      offset = offset + page_size
      logger.info(s"Current page set to $current_page (of $page_amount). Offset - $offset. TotalRowCount - $total_amount.")
    }}

  def set_total_amount(row_amount: BigInt): Unit = {
    total_amount = row_amount
    page_amount = (row_amount / page_size) + 1
    logger.info(s"Total amount of rows in request set to $total_amount")
    logger.info(s"Page amount set to $page_amount rows")
  }

  }

