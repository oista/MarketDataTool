import com.typesafe.config.ConfigFactory
import java.sql.{DriverManager, ResultSet}


object PGWorker extends App {

  val config  = ConfigFactory.load()
  val url  = config.getString("pg_url")
  val user = config.getString("pg_user")
  val pw   = config.getString("pg_pass")
  val TableName  = "users"


   val connection = DriverManager.getConnection(url, user, pw )
  val tname = "T_IND_ONLINE_ARBITRAGE"
 // val tname = "T_IND_BATCH_PERIOD"
  delete_table(tname)
  create_table(tname)

  def delete_table(dml:String): Unit = {
    val ddl =s"DROP TABLE $dml"
    connection.prepareStatement(ddl).execute()
  }

  def create_table(dml :String): Unit ={
  // (date varchar(50), ticker varchar(50), source_code varchar(50), price_difference decimal,      price_prc_difference varchar(50),  , batch_id bigint);"
  // (date varchar(50), ticker varchar(50), source_code varchar(50), price_difference varchar(500), price_prc_difference varchar(500),  batch_id varchar(50));"
  val ddl = s"CREATE TABLE $dml(date varchar(50), ticker varchar(50), source_code varchar(50), price_difference varchar(500), price_prc_difference varchar(500), batch_id varchar(500));"
 // val ddl = s"CREATE TABLE $dml(exchange varchar(50), symbol varchar(50), period_start varchar(50), period_end varchar(50), avg_price numeric(500), top_price numeric(500), low_price  numeric(500), period_type varchar(10));"
    connection.prepareStatement(ddl).execute()

  }


}

