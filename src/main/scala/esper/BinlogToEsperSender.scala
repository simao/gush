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
      .flatMapIterable(s => BinlogEvent.parseAll(s))
  }
}

class BinlogToEsperSender(cepService: EPServiceProvider, config: GushConfig) extends StatsdSender with StrictLogging {
  def sendToEsper(event: BinlogEvent): Unit = {
    logger.debug(s"Sending event for table ${event.tableName} to ESPER")
    val esperEvent = new BinlogEsperEvent(event.tableName, event.fields)
    cepService.getEPRuntime.sendEvent(esperEvent)
  }

  def remoteStream = {
    new BinlogEventStream(new BinlogRemoteReader(config))
  }

  def handleStreamError[U](ex: Throwable): Observable[U] = {
    logger.error("Error: ", ex)
    statsd.increment("gush.exceptions.onError")
    Observable.empty
    // TODO: Should reconnect ...
  }

  def init = {
    remoteStream
      .inserts
      .onErrorResumeNext(handleStreamError _)
      .subscribe(sendToEsper _)
  }
}
