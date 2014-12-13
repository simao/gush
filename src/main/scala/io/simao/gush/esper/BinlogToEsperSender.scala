package io.simao.gush.esper

import com.espertech.esper.client.EPRuntime
import com.typesafe.scalalogging.StrictLogging
import io.simao.gush.binlog._
import io.simao.gush.util.{GushConfig, StatsdSender}
import rx.lang.scala.Observable

class BinlogToEsperSender(epRuntime: EPRuntime, config: GushConfig) extends StatsdSender with StrictLogging {

  def sendToEsper(event: BinlogEvent): Unit = epRuntime.sendEvent(event)

  def events(sqlStream: Observable[String]): Observable[BinlogEvent] = {
    sqlStream
      .filter(s ⇒ s.startsWith("INSERT INTO") || s.startsWith("UPDATE"))
      .filter(!_.contains("ON DUPLICATE KEY UPDATE"))
      .filter(!ignored_event(_))
      .flatMapIterable(BinlogEvent.parseAll(_).get)
  }

  def remoteStream: Observable[String] = {
    BinlogRemoteReader.events(config)
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

  private def ignored_event(sqlStatement: String) = {
    config.ignored_tables.exists { tn => sqlStatement.contains(s"`$tn`") } ||
      config.ignored_prefixes.exists(sqlStatement.startsWith)
  }
}
