import com.espertech.esper.client.EPServiceProvider
import com.typesafe.scalalogging.StrictLogging
import esper._
import util.{GushConfig, StatsdSender}

import scala.annotation.tailrec
import scala.util.Try

class Gush(implicit config: GushConfig) extends StatsdSender with StrictLogging {
  def startCrunching() = {
    statsd.increment("gush.startup")
    val cepService = (new EsperEventListenersManager).init
    startBinlogLoop(cepService)
  }

  @tailrec
  final def startBinlogLoop(cepService: EPServiceProvider): Unit = {
    Try(new BinlogToEsperSender(cepService, config).init) recover {
      case ex =>
        statsd.increment("gush.exceptions.loop")
        logger.error("Error occurred: ", ex)
    }

    startBinlogLoop(cepService)
  }
}

object GushApp extends StatsdSender {
  def main(args: Array[String]) {
    (new Gush).startCrunching()
  }
}
