package io.simao.gush.esper

import com.espertech.esper.client.EPRuntime
import com.typesafe.scalalogging.StrictLogging
import io.simao.gush.binlog._
import io.simao.gush.util.{GushConfig, StatsdSender}
import rx.lang.scala.Observable

// TODO: onError should not resume, just bubble up and something should cause a reconnect
// TODO: .get should be handled differently, it's easy because sendToEsper can expect a try and raise
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
}

class BinlogToEsperSender(epRuntime: EPRuntime, config: GushConfig) extends StatsdSender with StrictLogging {
  def sendToEsper(event: BinlogEvent): Unit = {
    epRuntime.sendEvent(event)
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
