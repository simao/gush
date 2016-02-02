package io.simao.gush.akka_streams

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, SourceQueue}
import io.simao.gush.binlog.BinlogEvent
import io.simao.gush.util.GushConfig
import rx.lang.scala.Observable


class BinlogToAkkaStreams(eventSource: Observable[String], config: GushConfig) {
  def sqlStream: Source[String, SourceQueue[String]] = Source.queue[String](100, OverflowStrategy.dropNew)

  def events: Flow[String, BinlogEvent, Unit] = {
    Flow[String]
      .filter(s ⇒ s.startsWith("INSERT INTO") || s.startsWith("UPDATE"))
      .filter(!_.contains("ON DUPLICATE KEY UPDATE"))
      .filter(!ignored_event(_))
      .mapConcat(BinlogEvent.parseAll(_).get)
  }

  def startSending[T](sink: Sink[BinlogEvent, T])(implicit materializer: Materializer): T  = {
    val (queue, stream) =
      sqlStream
        .via(events)
        .toMat(sink)(Keep.both)
        .run()

    val subscribeFn: String ⇒ Unit = queue.offer(_)

    eventSource.subscribe(subscribeFn)

    stream
  }

  private def ignored_event(sqlStatement: String) = {
    config.ignored_tables.exists { tn => sqlStatement.contains(s"`$tn`") } ||
      config.ignored_prefixes.exists(sqlStatement.startsWith)
  }
}
