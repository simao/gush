package esper

import binlog._
import esper._

import com.espertech.esper.client.EPServiceProvider
import rx.lang.scala.Observable

trait BinlogSqlStream {
  def events: Observable[String]
}

class BinlogEventStream(eventStream: BinlogSqlStream) {
  def inserts: Observable[BinlogEvent] = {
    // TODO: Not parsing updates
    eventStream.events.filter(s => s.startsWith("INSERT INTO"))
      .filter(s => !s.contains("ON DUPLICATE KEY UPDATE"))
      .map(s => BinlogEvent(s))
  }
}

class BinlogToEsper(cepService: EPServiceProvider) {
  def sendToEsper(event: BinlogEvent) = {
    val esperEvent = new BinlogStreamEvent(event.tableName, event.fields)
    cepService.getEPRuntime.sendEvent(esperEvent)
  }

  def localStream = {
    new BinlogEventStream(new BinlogFileReader("mysql-bin.001050"))
  }

  def remoteStream = {
    new BinlogEventStream(new BinlogRemoteReader("127.0.0.1", 9797, "replication", ""))
  }

  def init = {
    //val stream = remoteStream
    val stream = localStream
    val o = stream.inserts

    o.subscribe { e => { println(e) } }
  }
}
