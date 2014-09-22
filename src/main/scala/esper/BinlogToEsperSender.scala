package esper

import binlog._
import com.espertech.esper.client.EPServiceProvider
import com.typesafe.scalalogging.StrictLogging
import rx.lang.scala.Observable
import util.{GushConfig, StatsdSender}

trait BinlogSqlStream {
  def events: Observable[String]
}

class BinlogEventStream(eventStream: BinlogSqlStream) {
  def inserts: Observable[BinlogEvent] = {
    eventStream.events
      .filter(s => s.startsWith("INSERT INTO"))
      .filter(s => !s.contains("ON DUPLICATE KEY UPDATE"))
      .map(s => BinlogEvent(s))
  }
}

class BinlogToEsperSender(cepService: EPServiceProvider, config: GushConfig) extends StatsdSender with StrictLogging {
  def sendToEsper(event: BinlogEvent) = {
    logger.debug(s"Sending event for table ${event.tableName} to ESPER")
    val esperEvent = new BinlogEsperEvent(event.tableName, event.fields)
    cepService.getEPRuntime.sendEvent(esperEvent)
  }

  def remoteStream = {
    new BinlogEventStream(new BinlogRemoteReader(config))
  }

  def init = {
    val stream = remoteStream
    val o = stream.inserts

    o.onErrorFlatMap({(ex, value) => {
      value.map(v => logger.error(s"Error processing: $v"))
      logger.error("Error: ", ex)
      statsd.increment("gush.exceptions.onError")

      Observable.empty

      // TODO: No reconnect ...
      }
    }).subscribe(
      { binlog_event => sendToEsper(binlog_event) }
    )
  }
}
