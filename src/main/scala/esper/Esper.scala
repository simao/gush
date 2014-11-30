package esper

import com.espertech.esper.client.{Configuration, EPServiceProvider, EPServiceProviderManager}

object Esper {
  def setup: EPServiceProvider = {
    val config = new Configuration()
    config.addEventType("BinlogInsertEvent", classOf[BinlogInsertEvent].getName)
    config.addEventType("BinlogUpdateEvent", classOf[BinlogUpdateEvent].getName)
    EPServiceProviderManager.getDefaultProvider(config)
  }
}
