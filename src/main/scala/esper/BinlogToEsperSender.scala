package esper

import binlog._
import esper._

import com.espertech.esper.client.EPServiceProvider
import rx.lang.scala.Observable

import com.typesafe.scalalogging.log4j._

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

class BinlogToEsperSender(cepService: EPServiceProvider, user: String, password: String, host: String, port: Int) extends Logging {
  def sendToEsper(event: BinlogEvent) = {
    logger.debug(s"Sending event for table ${event.tableName} to ESPER")
    val esperEvent = new BinlogEsperEvent(event.tableName, event.fields)
    cepService.getEPRuntime.sendEvent(esperEvent)
  }

  def localStream = {
    new BinlogEventStream(new BinlogFileReader("mysql-bin.001050"))
  }

  def remoteStream = {
    new BinlogEventStream(new BinlogRemoteReader(host, port, user, password))
  }

  def init = {
   // val stream = localStream
    val stream = remoteStream
    val o = stream.inserts

    o.onErrorFlatMap({(ex, value) => {
      value.map(v => logger.error("Error processing: ${v}"))
      logger.error("Error: ", ex)
      Observable.empty

      // TODO: No retry...
      }
    }).subscribe(
      { binlog_event => sendToEsper(binlog_event) },
      {ex => logger.error("onError => ${ex.message}")}
    )
  }
}
