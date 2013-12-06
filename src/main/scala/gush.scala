import binlog._
import esper._

import org.apache.log4j.Level
import scala.collection.JavaConversions._
import com.espertech.esper.client.{Configuration, EPServiceProvider, EPServiceProviderManager}

object Esper {
  def setup: EPServiceProvider = {
    val config = new Configuration()
    config.addEventType("BinlogStreamEvent", classOf[BinlogStreamEvent].getName)
    EPServiceProviderManager.getDefaultProvider(config)
  }
}

class Gush {
  def startCrunching = {
    val cepService = Esper.setup

    (new StreamEventListenersManager).init(cepService)

    // (new BinlogToEsper(cepService)).init
    (new BinlogToEsper(cepService)).init
  }
}

object GushApp {
  def main(args: Array[String]) {

    (new Gush).startCrunching
  }
}
