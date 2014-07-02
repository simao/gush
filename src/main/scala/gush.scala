import binlog._
import esper._
import com.typesafe.scalalogging.log4j._
import util.GushConfig

class Gush {
  def startCrunching(implicit config: GushConfig) = {
    val user = config("user")
    val password = config("password")
    val host = config("host")
    val port = config("port").toInt

    val cepService = (new EsperEventListenersManager).init

    new BinlogToEsperSender(cepService, user, password, host, port).init
  }
}

object GushApp extends Logging {

  def main(args: Array[String]) {
    try {
      (new Gush).startCrunching
    } catch {
      case ex: Throwable => {
        logger.error("Fatal error occurred: ", ex)
        throw ex
      }
    }
  }
}
