package esper

import com.espertech.esper.client.{EPServiceProvider, EventBean, UpdateListener}
import com.typesafe.scalalogging.StrictLogging
import rx.lang.scala.{Observable, Observer, Subscription}
import util._

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
    (new NewUsersCount).init(builder)
    (new ReviewsAvg).init(builder)
    (new ReviewsCount).init(builder)

    epService
  }
}

class EventObserverBuilder(epService: EPServiceProvider) extends StrictLogging {
  def createEsperListener(expression: String, callback: EventBean => Unit) = {
    val statement = epService.getEPAdministrator.createEPL(expression)

    val listener = new UpdateListener() {
      override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
        newEvents foreach (callback(_))
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
    val exp = s"SELECT count(*) as count FROM BinlogEsperEvent.win:time($intervalSecs second) where tableName='${table_name}' OUTPUT LAST EVERY 30 SECONDS"

    event_builder
      .observer(exp)
      .map(e => Option(e.get("count")).map(x => x.asInstanceOf[Long].toDouble))
      .subscribe { count =>
        count.map(x => statsd.gauge(statsd_key_name, x.toInt))
        logger.info(s"Number of ${table_name} in last ${intervalSecs} seconds: ${count}")
    }
  }
}

abstract class WindowedAvg(interval: Duration) extends WindowedEvent(interval) with StatsdSender with StrictLogging {

  def table_name: String
  def field_name: String
  def statsd_key_name: String

  def init(event_builder: EventObserverBuilder) = {
    val exp = s"SELECT avg(asFloat('${field_name}')) as ${field_name} FROM BinlogEsperEvent.win:time($intervalSecs second) where tableName='${table_name}' OUTPUT LAST EVERY 30 SECONDS"

    event_builder
      .observer(exp)
      .map(e => Option(e.get(field_name)).map(x => x.asInstanceOf[Double]))
      .subscribe { avg => avg.map(x => {
          statsd.gauge(statsd_key_name, x)
          logger.info(s"Avg ${field_name} of last ${intervalSecs} seconds: ${avg}")
      })
    }
  }

}

abstract class EventCount extends StatsdSender with StrictLogging {
  def table_name: String
  def statsd_key_name: String

  def init(event_builder: EventObserverBuilder) = {
    val exp = s"SELECT * FROM BinlogEsperEvent where tableName='$table_name'"

    event_builder
      .observer(exp)
      .map(_ => statsd.increment(statsd_key_name))
      .subscribe { _ => logger.info(s"New $table_name received") }
  }
}

class NewBookingCount extends EventCount {
  def table_name = "bookings"
  def statsd_key_name = "gush.bookings.total"
}

class BookingAvgNetRevenue extends WindowedAvg(1 hour) {
  def table_name = "booking_pricing_details"
  def field_name = "net_revenue"
  def statsd_key_name = "gush.bookings.net_revenue.avg_1hour"
}

class BookingsCount extends WindowedCount(1 hour) {
  def table_name = "bookings"
  def statsd_key_name = "gush.bookings.count_1hour"
}

class NewUsersCount extends WindowedCount(1 hour) {
  def table_name = "users"
  def statsd_key_name = "gush.users.count_1hour"
}

class ReviewsAvg extends WindowedAvg(1 hour) {
  def table_name = "reviews"
  def field_name = "rating"
  def statsd_key_name = "gush.reviews.rating.avg_1hour"
}

class ReviewsCount extends WindowedCount(1 hour) {
  def table_name = "reviews"
  def statsd_key_name = "gush.reviews.count_1hour"
}
