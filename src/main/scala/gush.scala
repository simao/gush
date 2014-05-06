import binlog._
import esper._

import org.apache.log4j.Level
import scala.collection.JavaConversions._
import com.espertech.esper.client.{Configuration, EPServiceProvider, EPServiceProviderManager}
import org.yaml.snakeyaml.Yaml
import java.io.{FileInputStream, File}

import com.typesafe.scalalogging.log4j._

object Esper {
  def setup: EPServiceProvider = {
    val config = new Configuration()
    config.addEventType("BinlogStreamEvent", classOf[BinlogStreamEvent].getName)
    EPServiceProviderManager.getDefaultProvider(config)
  }
}

class Gush {
  def startCrunching(config: Map[String, String]) = {
    val user = config("user")
    val password = config("password")
    val host = config("host")
    val port = config("port").toInt

    val cepService = Esper.setup

    new StreamEventListenersManager(cepService).init

    new BinlogToEsper(cepService, user, password, host, port).init
  }
}

object GushApp extends Logging {

  def loadConfig(path: String) = {
    val input = new FileInputStream(new File(path))
    val yaml = new Yaml()
    val data = yaml.load(input)

    data.asInstanceOf[java.util.Map[String, String]].toMap
  }


  def main(args: Array[String]) {
    try {
      val config = loadConfig("gush.config.yml")

      (new Gush).startCrunching(config)
    } catch {
      case ex: Throwable => {
        logger.error("Fatal error occurred: ", ex)
        throw ex
      }
    }
  }
}
