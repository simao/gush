package esper

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.Console._

import com.espertech.esper.client.{EventBean, UpdateListener, EPServiceProvider}
import com.typesafe.scalalogging.log4j._

class StreamEventListenersManager {
  def init(epService: EPServiceProvider) = {
    (new NewBookingEventListener).init(epService)
//      (new NewBookingPricingListener).init(epService)
      (new BookingAvgNetRevenue(10 seconds)).init(epService)
      (new BookingCountWindow).init(epService)
  }
}

// TODO: Each listener should be an Observable that everyone can subscribe to
// - Then we can compose streams

abstract class BinlogStreamEventListener extends UpdateListener  {
  def init(epService: EPServiceProvider) = {
    val statement = epService.getEPAdministrator.createEPL(expression)
    statement.addListener(this)
  }

  def expression: String
}

class NewBookingEventListener extends BinlogStreamEventListener {
  override def expression = "SELECT * FROM BinlogStreamEvent where tableName='bookings'"

  def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
    val event = newEvents(0)
    //println(event.get("field('net_revenue')"))
  }
}

class NewBookingPricingListener extends BinlogStreamEventListener {
  override def expression = "SELECT * FROM BinlogStreamEvent where tableName='booking_pricing_details'"

  def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
    val event = newEvents(0)
    //    println(event.get("field('net_revenue')"))
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

class BookingAvgNetRevenue(interval: Duration = 10 seconds) extends WindowedEvent(interval) {
  override def expression = {
    val inSecs = interval.toSeconds

    s"SELECT avg(asFloat('net_revenue')) as avg_rev FROM BinlogStreamEvent.win:time_batch($inSecs second) where tableName='booking_pricing_details'"
  }

  def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
    assert(newEvents.size == 1)
    val event = newEvents(0)
    val avg = Option(event.get("avg_rev")).map(x => x.asInstanceOf[Double])

    println(s"Avg net revenue of last ${interval.toSeconds} seconds: ${colorize(avg)}")

    avg.map(x => lastVal = x)
  }
}

class BookingCountWindow(interval: Duration = 10 seconds) extends WindowedEvent(interval) {
  override def expression = {
    val inSecs = interval.toSeconds

    s"SELECT count(*) as count FROM BinlogStreamEvent.win:time_batch($inSecs second) where tableName='bookings'"
  }

  def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
    assert(newEvents.size == 1)
    val event = newEvents(0)
    val count = Option(event.get("count")).map(x => x.asInstanceOf[Long].toDouble)

    println(s"Number of bookings in last ${interval.toSeconds} seconds: ${colorize(count)}")

    count.map(x => lastVal = x)
  }
}
