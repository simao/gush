package io.simao.gush.akka_streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, Sink}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import io.simao.gush.SqlToBinlog
import io.simao.gush.binlog.{BinlogEvent, BinlogInsertEvent, BinlogUpdateEvent}
import io.simao.gush.util.GushConfig
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSuiteLike, ShouldMatchers}

import scala.collection.JavaConverters._

class BinlogToAkkaTest extends
  TestKit(ActorSystem("BinlogAkkaTestKit"))
  with FunSuiteLike
  with ScalaFutures
  with ShouldMatchers  {
  implicit val materializer = ActorMaterializer()

  val config = GushConfig.default

  val sqlQueries = List(
    "INSERT INTO `table0` (`col0`, `col1`, `col2`) VALUES (1, 2, 3)",
    "UPDATE `table1` SET col0 = 4 WHERE col1 = 1"
  )

  val sqlStream = Source(sqlQueries)

  val subject = new SqlToBinlog(sqlStream, config)

  test("sends insert events to akka") {
    val insertEvent = BinlogInsertEvent("table0", Map("col2" → "3", "col1" → "2", "col0" → "1"))

    val testSink = TestSink.probe[BinlogEvent]

    val next = subject.startSending(testSink).requestNext()

    next shouldBe insertEvent
  }

  test("sends UPDATE events to ESPER runtime") {
    val updateEvent = BinlogUpdateEvent("table1",
      Map("col0" → "4"),
      Map("col1" → "1"))
    val testSink = TestSink.probe[BinlogEvent]

    val probe = subject.startSending(testSink)
    probe.requestNext()
    val next = probe.requestNext()

    next shouldBe updateEvent
  }

  test("ignores sql statements prefixed with ignored prefixes") {
    val sqlQueries = List(
      "INSERT INTO `table0` (`col0`, `col1`, `col2`) VALUES (1, 2, 3)",
      "UPDATE `table1` SET col0 = 4 WHERE col1 = 1"
    )

    val config = new GushConfig(
      Map("ignored_statements_prefixes" → List("INSERT").asJava,
        "ignored_tables" → List().asJava))

    val queryStream = Source(sqlQueries)

    val subject = new SqlToBinlog(queryStream, config)

    val testSink = Sink.seq[BinlogEvent]

    val f = subject.startSending(testSink)

    whenReady(f) { probeContents ⇒
      probeContents should have size 1
      probeContents.head shouldBe a[BinlogUpdateEvent]
    }
  }

  test("recovers after an error") {
    pending

//    val subject = new BinlogToAkkaTest(epRuntime, config) {
//      var called = false
//      override def handleStreamError(ex: Throwable): Unit = {}
//
//      override def remoteStream = Observable(o ⇒ {
//        if(!called) {
//          called = true
//          o.onError(new Exception("Fails"))
//        }
//        o.onNext("INSERT INTO `table0` (`col1`) VALUES (1)")
//        o.onCompleted()
//      })
//    }
//
//    val insertEvent = BinlogInsertEvent("table0", Map("col1" → "1"))
//
//    subject.startEventSending
//
//    verify(epRuntime).sendEvent(org.mockito.Matchers.eq(insertEvent))
  }
}

