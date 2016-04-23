package io.simao.gush

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.simao.gush.binlog.BinlogEvent
import io.simao.gush.util.GushConfig

class SqlToBinlog[T](eventSource: Source[String, T], config: GushConfig) {
  def events: Flow[String, BinlogEvent, NotUsed] = {
    Flow[String]
      .filter(s ⇒ s.startsWith("INSERT INTO") || s.startsWith("UPDATE"))
      .filter(!_.contains("ON DUPLICATE KEY UPDATE"))
      .filter(!ignored_event(_))
      .mapConcat(e ⇒ BinlogEvent.parseAll(e).get) // TODO Don't use get
  }

  def startSending[S](sink: Sink[BinlogEvent, S])(implicit materializer: Materializer): S  = {
    eventSource
      .via(events)
      .runWith(sink)
  }

  private def ignored_event(sqlStatement: String) = {
    config.ignored_tables.exists { tn => sqlStatement.contains(s"`$tn`") } ||
      config.ignored_prefixes.exists(sqlStatement.startsWith)
  }
}
