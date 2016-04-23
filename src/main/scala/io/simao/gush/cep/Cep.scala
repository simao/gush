package io.simao.gush.cep

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import io.simao.gush.binlog.{BinlogEvent, BinlogInsertEvent, BinlogUpdateEvent}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object InsertEventFlow {
  def apply(tableName: String): Flow[BinlogEvent, BinlogInsertEvent, NotUsed] = {
    Flow[BinlogEvent]
      .filter(_.isInstanceOf[BinlogInsertEvent])
      .map(_.asInstanceOf[BinlogInsertEvent])
      .filter(_.tableName == tableName)
  }
}

object UpdateEventFlow {
  def apply(tableName: String): Flow[BinlogEvent, BinlogUpdateEvent, NotUsed] = {
    Flow[BinlogEvent]
      .filter(_.isInstanceOf[BinlogEvent])
      .map(_.asInstanceOf[BinlogUpdateEvent])
      .filter(_.tableName == tableName)
  }
}

object WindowedInsertCount {
  def apply(tableName: String, interval: FiniteDuration)(op: Seq[BinlogEvent] ⇒ Unit): Sink[BinlogEvent, Future[Done]] = {
    val f = InsertEventFlow(tableName).groupedWithin(Int.MaxValue, interval)
    val sink = Sink.foreach[Seq[BinlogEvent]](op)
    f.toMat(sink)(Keep.right)
  }
}

object WindowedInsertAvg {
  def apply(tableName: String, fieldName: String, interval: FiniteDuration)(op: Option[Float] ⇒ Unit): Sink[BinlogEvent, Future[Done]] = {
    val f =
      InsertEventFlow(tableName)
        .map(_.getAsFloat(fieldName))
        .groupedWithin(Integer.MAX_VALUE, interval)
        .map(e ⇒ Try(e.foldLeft(0f)(_ + _) / e.size).toOption)

    val sink = Sink.foreach[Option[Float]](op)

    f.toMat(sink)(Keep.right)
  }
}
