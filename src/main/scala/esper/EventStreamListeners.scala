package esper

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.Console._

import com.espertech.esper.client.{EventBean, UpdateListener, EPServiceProvider}
import com.typesafe.scalalogging.log4j._

import util.StatsdSender;

class StreamEventListenersManager(epService: EPServiceProvider) extends Logging {
  def init = {
    logger.info("Initializing esper event listener")

    (new NewBookingEventListener(epService)).init
    (new BookingAvgNetRevenue(epService, 10 seconds)).init
    (new BookingCountWindow(epService)).init
  }
}

abstract class EventStreamListener(epService: EPServiceProvider) extends UpdateListener with Logging with StatsdSender {
  def update(newsEvents: Array[EventBean]): Unit

  def expression: String

  override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
    update(newEvents)
    // TODO: oldEvents ignored
  }

  def init() = {
    val statement = epService.getEPAdministrator.createEPL(expression)
    statement.addListener(this)
    logger.info(s"Subscriber for $expression initialized")
  }
}

abstract class WindowedEventStreamListener(epService: EPServiceProvider, interval: Duration = 5 seconds) extends EventStreamListener(epService) {
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

class NewBookingEventListener(epService: EPServiceProvider) extends EventStreamListener(epService) {
  def expression = "SELECT * FROM BinlogStreamEvent where tableName='bookings'"

  def update(newEvents: Array[EventBean]) = {
    for(e <- newEvents) {
      statsd.increment("gush.booking.total")
      logger.info("New booking received")
    }
  }
}

class BookingAvgNetRevenue(epService: EPServiceProvider, interval: Duration = 10 seconds) extends WindowedEventStreamListener(epService, interval) {

  def expression = s"SELECT avg(asFloat('net_revenue')) as avg_rev FROM BinlogStreamEvent.win:time_batch($intervalSecs second) where tableName='booking_pricing_details'"

  def update(newEvents: Array[EventBean]) = {
    for(e <- newEvents) {
      val avg = Option(e.get("avg_rev")).map(x => x.asInstanceOf[Double] / 1000)

      avg.map(x => {
        lastVal = x
        statsd.gauge("gush.booking.avg_revenue_10seconds", x)
      })

      logger.info(s"Avg net revenue of last ${intervalSecs} seconds: ${colorize(avg)}")
    }
  }
}

class BookingCountWindow(epService: EPServiceProvider, interval: Duration = 10 seconds) extends WindowedEventStreamListener(epService, interval) {
  def expression = s"SELECT count(*) as count FROM BinlogStreamEvent.win:time_batch($intervalSecs second) where tableName='bookings'"

  def update(newEvents: Array[EventBean]) = {
    for(e <- newEvents) {
      val count = Option(e.get("count")).map(x => x.asInstanceOf[Long].toDouble)

      count.map(x => lastVal = x)
      count.map(x => statsd.increment("gush.booking.count_10seconds", x.toInt))

      logger.info(s"Number of bookings in last ${intervalSecs} seconds: ${colorize(count)}")
    }
  }
}
