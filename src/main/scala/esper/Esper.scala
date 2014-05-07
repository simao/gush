import com.espertech.esper.client.{Configuration, EPServiceProvider, EPServiceProviderManager}

import binlog._

object Esper {
  def setup: EPServiceProvider = {
    val config = new Configuration()
    config.addEventType("BinlogStreamEvent", classOf[BinlogStreamEvent].getName)
    EPServiceProviderManager.getDefaultProvider(config)
  }
}
