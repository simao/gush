package esper

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.Console._

import rx.lang.scala.{Observable, Subscription, Observer}

import com.espertech.esper.client.{EventBean, UpdateListener, EPServiceProvider}
import com.typesafe.scalalogging.log4j._

import util._

class StreamEventListenersManager(epService: EPServiceProvider) extends Logging {
  def init = {
    logger.info("Initializing esper event listener")

    val builder = new EventObserverBuilder(epService)

    (new NewBookingEventListener).init(builder)
    (new BookingAvgNetRevenue(10 seconds)).init(builder)
    (new BookingCountWindow).init(builder)
  }
}

class EventObserverBuilder(epService: EPServiceProvider) extends Logging {
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

abstract class WindowedEvent(val interval: Duration = 5 seconds) {
  var lastVal = 0.0

  val intervalSecs = interval.toSeconds

  def colorize(in: Option[Double]) = {
    in match {
      case Some(i) => if(i > lastVal)
        GREEN + i + RESET
      else if(i < lastVal)
        RED + i + RESET
      else
        YELLOW + i + RESET

      case None => BOLD + "ERROR" + RESET
    }
  }
}

class NewBookingEventListener extends StatsdSender with Logging {
  def init(event_builder: EventObserverBuilder) = {
    val exp = "SELECT * FROM BinlogStreamEvent where tableName='bookings'"
    event_builder
      .observer(exp)
      .map(_ => statsd.increment("gush.booking.total"))
      .subscribe { _ => logger.info("New booking received") }
  }
}

class BookingAvgNetRevenue(interval: Duration = 10 seconds) extends WindowedEvent(interval) with StatsdSender with Logging {
  def init(event_builder: EventObserverBuilder) = {
    val exp = s"SELECT avg(asFloat('net_revenue')) as avg_rev FROM BinlogStreamEvent.win:time_batch($intervalSecs second) where tableName='booking_pricing_details'"

    event_builder
      .observer(exp)
      .map(e => Option(e.get("avg_rev")).map(x => x.asInstanceOf[Double] / 1000))
      .subscribe { avg => avg.map(x => {
          lastVal = x
          statsd.gauge("gush.booking.avg_revenue_10seconds", x)
      })

      logger.info(s"Avg net revenue of last ${intervalSecs} seconds: ${colorize(avg)}")
    }
  }
}

class BookingCountWindow(interval: Duration = 10 seconds) extends WindowedEvent(interval) with StatsdSender with Logging {
  def init(event_builder: EventObserverBuilder) = {
    val exp = s"SELECT count(*) as count FROM BinlogStreamEvent.win:time_batch($intervalSecs second) where tableName='bookings'"

    event_builder
      .observer(exp)
      .map(e => Option(e.get("count")).map(x => x.asInstanceOf[Long].toDouble))
      .subscribe { count =>
        count.map(x => lastVal = x)
        count.map(x => statsd.increment("gush.booking.count_10seconds", x.toInt))
        logger.info(s"Number of bookings in last ${intervalSecs} seconds: ${colorize(count)}")
    }
  }
}
