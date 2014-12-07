package io.simao.gush.esper

import com.espertech.esper.client.{EPServiceProvider, EventBean, UpdateListener}
import com.typesafe.scalalogging.StrictLogging
import io.simao.gush.binlog.BinlogUpdateEvent
import rx.lang.scala.{Observable, Observer, Subscription}
import io.simao.gush.util._

import scala.concurrent.duration._
import scala.language.postfixOps

class EsperEventListenersManager extends StrictLogging {
  def init = {
    val epService = Esper.setup

    logger.info("Initializing esper event listener")

    val builder = new EventObserverBuilder(epService)

    (new NewBookingCount).init(builder)
    (new BookingAvgNetRevenue).init(builder)
    (new BookingsCount).init(builder)
    (new BookingsCountLong).init(builder)
    (new NewUsersCount).init(builder)
    (new ReviewsAvg).init(builder)
    (new ReviewsCount).init(builder)
    (new BookingUpdateCount).init(builder)

    epService
  }
}

class EventObserverBuilder(epService: EPServiceProvider) extends StrictLogging {
  def createEsperListener(expression: String, callback: EventBean => Unit) = {
    val statement = epService.getEPAdministrator.createEPL(expression)

    val listener = new UpdateListener() {
      override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
        newEvents foreach callback
      }
    }

    statement.addListener(listener)

    logger.info(s"Subscriber for $expression initialized")

    { () => statement.removeListener(listener) }
  }

  def observer(expression: String) = {
    Observable.create({observer: Observer[EventBean] =>
      val destroy_f = createEsperListener(expression, event => observer.onNext(event))
      Subscription { destroy_f() }
    })
  }
}

abstract class WindowedEvent(val interval: Duration) {
  val intervalSecs = interval.toSeconds
}

abstract class WindowedCount(interval: Duration) extends WindowedEvent(interval) with StatsdSender with StrictLogging {

  def table_name: String
  def statsd_key_name: String

  def init(event_builder: EventObserverBuilder) = {
    val exp = s"SELECT count(*) as count FROM BinlogInsertEvent.win:time($intervalSecs second) where tableName='$table_name' OUTPUT LAST EVERY 10 SECONDS"

    event_builder
      .observer(exp)
      .map(e => Option(e.get("count")).map(x => x.asInstanceOf[Long].toDouble))
      .subscribe { count =>
        count.map(x => statsd.gauge(statsd_key_name, x.toInt))
        logger.info(s"Number of $table_name in last $intervalSecs seconds: ${count.getOrElse(0)}")
    }
  }
}

abstract class WindowedAvg(interval: Duration) extends WindowedEvent(interval) with StatsdSender with StrictLogging {

  def table_name: String
  def field_name: String
  def statsd_key_name: String

  def init(event_builder: EventObserverBuilder) = {
    val exp = s"SELECT avg(asFloat('$field_name')) as $field_name FROM BinlogInsertEvent.win:time($intervalSecs second) where tableName='$table_name' OUTPUT LAST EVERY 10 SECONDS"

    event_builder
      .observer(exp)
      .map(e => Option(e.get(field_name))
      .map(_.asInstanceOf[Double]))
      .subscribe { avg => avg.map(x => {
          statsd.gauge(statsd_key_name, x)
          logger.info(s"Avg $field_name of last $intervalSecs seconds: $x")
      })
    }
  }

}

abstract class InsertEvent extends StatsdSender with StrictLogging {
  def table_name: String
  def statsd_key_name: String

  def init(event_builder: EventObserverBuilder) = {
    val exp = s"SELECT * FROM BinlogInsertEvent where tableName='$table_name'"

    event_builder
      .observer(exp)
      .map(_ => statsd.increment(statsd_key_name))
      .subscribe { _ => logger.info(s"New $table_name received") }
  }
}

abstract class UpdateEvent extends StatsdSender with StrictLogging {
  val table_name: String
  def init(event_builder: EventObserverBuilder) = {
    val exp = s"SELECT * FROM BinlogUpdateEvent where tableName='$table_name'"
    
    event_builder
      .observer(exp)
      .map(_.getUnderlying.asInstanceOf[BinlogUpdateEvent])
      .subscribe {
      e ⇒ {
        logger.info(s"Update to $table_name received: booking_id ${e.whereFields("id")}")
      }
    }
  }
}

class BookingUpdateCount extends UpdateEvent {
  override val table_name = "bookings"
}

class NewBookingCount extends InsertEvent {
  def table_name = "bookings"
  def statsd_key_name = "gush.bookings.total"
}

class BookingAvgNetRevenue extends WindowedAvg(1 hour) {
  def table_name = "booking_pricing_details"
  def field_name = "net_revenue"
  def statsd_key_name = "gush.bookings.net_revenue.avg_long"
}

class BookingsCount extends WindowedCount(1 hour) {
  def table_name = "bookings"
  def statsd_key_name = "gush.bookings.count_long"
}

class BookingsCountLong extends WindowedCount(2 minutes) {
  def table_name = "bookings"
  def statsd_key_name = "gush.bookings.count_short"
}

class NewUsersCount extends WindowedCount(1 hour) {
  def table_name = "users"
  def statsd_key_name = "gush.users.count_long"
}

class ReviewsAvg extends WindowedAvg(1 hour) {
  def table_name = "reviews"
  def field_name = "rating"
  def statsd_key_name = "gush.reviews.rating.avg_long"
}

class ReviewsCount extends WindowedCount(1 hour) {
  def table_name = "reviews"
  def statsd_key_name = "gush.reviews.count_long"
}
