package esper

import com.espertech.esper.client.{Configuration, EPServiceProvider, EPServiceProviderManager}

import binlog._

object Esper {
  def setup: EPServiceProvider = {
    val config = new Configuration()
    config.addEventType("BinlogEsperEvent", classOf[BinlogEsperEvent].getName)
    EPServiceProviderManager.getDefaultProvider(config)
  }
}
