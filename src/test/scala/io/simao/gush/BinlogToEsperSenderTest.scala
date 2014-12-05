package io.simao.gush

import com.espertech.esper.client.EPRuntime
import io.simao.gush.binlog.{BinlogInsertEvent, BinlogUpdateEvent}
import io.simao.gush.esper.BinlogToEsperSender
import io.simao.gush.util.GushConfig
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, OneInstancePerTest}
import rx.lang.scala.Observable


// TODO: Use scalamock. see https://github.com/paulbutcher/ScalaMock/issues/86
class BinlogToEsperSenderTest extends FunSuite with OneInstancePerTest with MockitoSugar {
  val config = GushConfig.default

  val epRuntime = mock[EPRuntime]

  val sqlQueries = List(
    "INSERT INTO `table0` (`col0`, `col1`, `col2`) VALUES (1, 2, 3)",
    "UPDATE `table1` SET col0 = 4 WHERE col1 = 1"
  )

  val sqlStream = Observable.from(sqlQueries)

  val subject = new BinlogToEsperSender(epRuntime, config) {
    override def remoteStream = sqlStream
  }

  test("sends insert events to ESPER runtime") {
    val insertEvent = BinlogInsertEvent("table0", Map("col2" → "3", "col1" → "2", "col0" → "1"))

    subject.startEventSending

    verify(epRuntime).sendEvent(org.mockito.Matchers.eq(insertEvent))
  }

  test("sends UPDATE events to ESPER runtime") {
    val updateEvent = BinlogUpdateEvent("table1",
      Map("col0" → "4"),
      Map("col1" → "1"))

    subject.startEventSending

    verify(epRuntime, atLeastOnce()).sendEvent(org.mockito.Matchers.eq(updateEvent))
  }

  test("recovers after an error") {
    val subject = new BinlogToEsperSender(epRuntime, config) {
      var called = false
      override def remoteStream = Observable(o ⇒ {
        // TODO: How to do this without state?
        if(!called) {
          called = true
          o.onError(new Exception("Fails"))
        }
        o.onNext("INSERT INTO `table0` (`col1`) VALUES (1)")
        o.onCompleted()
      })
    }
    val insertEvent = BinlogInsertEvent("table0", Map("col1" → "1"))

    subject.startEventSending

    verify(epRuntime).sendEvent(org.mockito.Matchers.eq(insertEvent))
  }
}
