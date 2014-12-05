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
class BinlogToEsperSender(epRuntime: EPRuntime, config: GushConfig) extends StatsdSender with StrictLogging {
  def sendToEsper(event: BinlogEvent): Unit = {
    epRuntime.sendEvent(event)
  }

  def events(sqlStream: Observable[String]): Observable[BinlogEvent] = {
    sqlStream
      .filter(s ⇒ s.startsWith("INSERT INTO") || s.startsWith("UPDATE"))
      .filter(!_.contains("ON DUPLICATE KEY UPDATE"))
      .flatMapIterable(BinlogEvent.parseAll(_).get)
  }

  def remoteStream: Observable[String] = {
    new BinlogRemoteReader(config).events
  }

  def handleStreamError(ex: Throwable): Unit = {
    logger.error("Error: ", ex)
    statsd.increment("gush.exceptions.onError")
  }

  def startEventSending = {
    val stream = events(remoteStream)
      .doOnError(handleStreamError _)
      .retry(3)
      .subscribe(sendToEsper _)

    stream
  }
}
