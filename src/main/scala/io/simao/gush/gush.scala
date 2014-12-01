package io.simao.gush


import com.espertech.esper.client.EPRuntime
import com.typesafe.scalalogging.StrictLogging
import io.simao.gush.esper.{BinlogToEsperSender, EsperEventListenersManager}
import io.simao.gush.util.{GushConfig, StatsdSender}

import scala.annotation.tailrec
import scala.util.Try

class Gush(implicit config: GushConfig) extends StatsdSender with StrictLogging {
  def start() = {
    statsd.increment("gush.startup")
    val cepService = (new EsperEventListenersManager).init
    startBinlogLoop(cepService.getEPRuntime)
  }

  @tailrec
  final def startBinlogLoop(epRuntime: EPRuntime): Unit = {
    Try(new BinlogToEsperSender(epRuntime, config).startEventSending) recover {
      case ex =>
        statsd.increment("gush.exceptions.loop")
        logger.error("Error occurred: ", ex)
    }

    startBinlogLoop(epRuntime)
  }
}

object GushApp extends StatsdSender {
  def main(args: Array[String]) {
    (new Gush).start()
  }
}
