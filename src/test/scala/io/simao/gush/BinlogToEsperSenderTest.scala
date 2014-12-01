package io.simao.gush

import com.espertech.esper.client.EPRuntime
import io.simao.gush.esper.{BinlogEventStream, BinlogToEsperSender}
import io.simao.gush.util.GushConfig
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import rx.lang.scala.Observable


// TODO: TEst properly
// TODO: Use scalamock. see https://github.com/paulbutcher/ScalaMock/issues/86
class BinlogToEsperSenderTest extends FunSuite with MockitoSugar {
  def config = GushConfig.default

  def epRuntime = mock[EPRuntime]

  def sqlStream: Observable[String] = Observable.empty

  def subject = new BinlogToEsperSender(epRuntime, config) {
        override def remoteStream: BinlogEventStream = new BinlogEventStream(sqlStream)
  }

  test("whats up") {
    subject.init
  }
}
