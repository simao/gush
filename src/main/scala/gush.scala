import com.espertech.esper.client.EPServiceProvider
import com.typesafe.scalalogging.StrictLogging
import esper._
import util.{GushConfig, StatsdSender}

class Gush(implicit config: GushConfig) extends StatsdSender with StrictLogging {
  def startCrunching = {
    statsd.increment("gush.startup")
    val cepService = (new EsperEventListenersManager).init
    startBinlogLoop(cepService)
  }

  def startBinlogLoop(cepService: EPServiceProvider): Unit = {
    val user = config("user")
    val password = config("password")
    val host = config("host")
    val port = config("port").toInt

    try {
      new BinlogToEsperSender(cepService, user, password, host, port).init
    } catch {
      case ex: Throwable => {
        statsd.increment("gush.exceptions.loop")
        logger.error("Error occurred: ", ex)
      }
    }

    startBinlogLoop(cepService)
  }
}

object GushApp extends StatsdSender {
  def main(args: Array[String]) {
    (new Gush).startCrunching
  }
}
