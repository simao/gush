package esper

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.Console._

import com.espertech.esper.client.{EventBean, UpdateListener, EPServiceProvider}
import com.typesafe.scalalogging.log4j._

class StreamEventListenersManager extends Logging {
  def init(epService: EPServiceProvider) = {

    logger.info("Initializing esper event listener")

    (new NewBookingEventListener).init(epService)
    (new BookingAvgNetRevenue(10 seconds)).init(epService)
    (new BookingCountWindow).init(epService)
  }
}

abstract class BinlogStreamEventListener extends UpdateListener with Logging  {
  def init(epService: EPServiceProvider) = {
    val statement = epService.getEPAdministrator.createEPL(expression)
    statement.addListener(this)
  }

  def expression: String
}

class NewBookingEventListener extends BinlogStreamEventListener with StatsdSender {
  override def expression = "SELECT * FROM BinlogStreamEvent where tableName='bookings'"

  def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
    val event = newEvents(0)
    logger.info("New booking received")
    statsd.increment("gush.booking.total")
  }
}

abstract class WindowedEvent(val interval: Duration = 5 seconds) extends BinlogStreamEventListener {
  var lastVal = 0.0

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

class BookingAvgNetRevenue(interval: Duration = 10 seconds) extends WindowedEvent(interval) with StatsdSender {
  override def expression = {
    val inSecs = interval.toSeconds

    s"SELECT avg(asFloat('net_revenue')) as avg_rev FROM BinlogStreamEvent.win:time_batch($inSecs second) where tableName='booking_pricing_details'"
  }

  def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
    assert(newEvents.size == 1)
    val event = newEvents(0)
    val avg = Option(event.get("avg_rev")).map(x => x.asInstanceOf[Double])

    logger.info(s"Avg net revenue of last ${interval.toSeconds} seconds: ${colorize(avg)}")

    avg.map(x => statsd.gauge("gush.booking.avg_revenue_10seconds", x))
    avg.map(x => lastVal = x)
  }
}

class BookingCountWindow(interval: Duration = 10 seconds) extends WindowedEvent(interval) with StatsdSender {
  override def expression = {
    val inSecs = interval.toSeconds

    s"SELECT count(*) as count FROM BinlogStreamEvent.win:time_batch($inSecs second) where tableName='bookings'"
  }

  def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
    assert(newEvents.size == 1)
    val event = newEvents(0)
    val count = Option(event.get("count")).map(x => x.asInstanceOf[Long].toDouble)

    logger.info(s"Number of bookings in last ${interval.toSeconds} seconds: ${colorize(count)}")

    count.map(x => statsd.increment("gush.booking.count_10seconds", x.toInt))
    count.map(x => lastVal = x)
  }
}
