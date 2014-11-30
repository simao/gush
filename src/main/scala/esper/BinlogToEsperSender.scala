package esper

import binlog._
import com.espertech.esper.client.EPServiceProvider
import com.typesafe.scalalogging.StrictLogging
import rx.lang.scala.Observable
import util.{GushConfig, StatsdSender}

// TODO: .get should be handled differently, maybe using `onError`?
// TODO: Needs tests
// TODO: Ignore skipping should be done here
class BinlogEventStream(sqlStream: Observable[String]) {
  def inserts: Observable[BinlogEvent] = {
    sqlStream
      .filter(_.startsWith("INSERT INTO"))
      .filter(!_.contains("ON DUPLICATE KEY UPDATE"))
      .flatMapIterable(BinlogEvent.parseAll(_).get)
  }

  def updates: Observable[BinlogEvent] = {
    sqlStream
      .filter(_.startsWith("UPDATE"))
      .flatMapIterable(BinlogEvent.parseAll(_).get)
  }

  def all: Observable[String] = sqlStream
}

class BinlogToEsperSender(cepService: EPServiceProvider, config: GushConfig) extends StatsdSender with StrictLogging {
  def sendToEsper(event: BinlogEvent): Unit = {
    println("sendiing to esper: " + event.toString)
    cepService.getEPRuntime.sendEvent(event)
  }

  def remoteStream: BinlogEventStream = {
    new BinlogEventStream(new BinlogRemoteReader(config).events)
  }

  def handleStreamError[U](ex: Throwable): Observable[U] = {
    logger.error("Error: ", ex)
    statsd.increment("gush.exceptions.onError")
    Observable.empty
    // TODO: Should reconnect ...
  }

  def init = {
    val stream = remoteStream.inserts.publish

    stream
      .onErrorResumeNext(handleStreamError _)
      .subscribe(sendToEsper _)

    stream.connect
  }
}
