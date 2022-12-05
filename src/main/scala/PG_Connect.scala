import MainProducer.logger
import com.typesafe.config.ConfigFactory

object PG_Connect {
  val config = ConfigFactory.load()
  val pg_url = config.getString("pg_url")
  val pg_usr = config.getString("pg_user")
  val pg_psw = config.getString("pg_pass")

  logger.info(s"PG url: $pg_url")
  logger.info(s"PG user: $pg_usr")
  logger.info(s"PG pass: $pg_psw")

}
