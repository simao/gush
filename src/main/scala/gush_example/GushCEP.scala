package gush_example

import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.StrictLogging
import io.simao.gush.binlog.{BinlogEvent, BinlogInsertEvent, BinlogUpdateEvent}
import io.simao.gush.cep.{InsertEventFlow, UpdateEventFlow, WindowedInsertAvg, WindowedInsertCount}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object GushCEP extends StrictLogging {
  def allSinks(): Sink[BinlogEvent, NotUsed] = {
    Sink.combine(
      log(),
      newBookingCount(),
      bookingsWindowCount(),
      bookingsWindowAvgRev(),
      bookingsUpdateCount())(Broadcast[BinlogEvent](_))
  }

  def log(): Sink[BinlogEvent, Future[Done]] = {
    Sink.foreach[BinlogEvent](e ⇒ logger.info(s"Received new event. (${e.tableName})"))
  }

  def newBookingCount(): Sink[BinlogEvent, Future[Done]] = {
    val f = InsertEventFlow("bookings")
    val sink = Sink.foreach[BinlogInsertEvent](e ⇒ logger.info(s"New ${e.tableName} received"))
    f.toMat(sink)(Keep.right)
  }

  def bookingsUpdateCount(): Sink[BinlogEvent, Future[Done]] = {
    val f = UpdateEventFlow("bookings")
    val sink = Sink.foreach[BinlogUpdateEvent](e ⇒ logger.info(s"Update to bookings received: booking_id ${e.whereFields("id")}"))
    f.toMat(sink)(Keep.right)
  }

  def bookingsWindowCount(): Sink[BinlogEvent, Future[Done]] = {
    WindowedInsertCount("bookings", 10 seconds)(e ⇒ logger.info(s"Number of bookings in last hour: ${e.size}"))
  }

  def bookingsWindowAvgRev(): Sink[BinlogEvent, Future[Done]] = {
    WindowedInsertAvg("bookings", "revenue", 10 seconds)(avg ⇒ logger.info(s"Average of last hour for bookings.avg is $avg"))
  }
}

