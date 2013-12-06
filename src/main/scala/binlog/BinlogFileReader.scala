package binlog

import binlog._
import esper._

import com.github.shyiko.mysql.binlog._
import com.github.shyiko.mysql.binlog.event._
import java.io._
import scala.util.{Try, Success, Failure}
import scala.language.implicitConversions

import rx.lang.scala._
import rx.lang.scala.ImplicitFunctionConversions
import rx.lang.scala.Observable

class BinlogFileReader(filename: String) extends BinlogSqlStream {
  val file = new File(filename)
  val binlogReader = new BinaryLogFileReader(file)

  override def events = {
    def _stream: Stream[String] = {
      println("_stream")

      readNextEvent match {
        case Some(sql) => Stream.cons(sql, _stream)
        case None => { println("OVER"); binlogReader.close; Stream.empty }
      }
    }

    // TODO: Will read everything once we call subscribe, it's not lazy
    Observable.from(_stream)
  }

  private def readNextEvent: Option[String] = {
    def reader(event: Event): Option[String] = Option(event).flatMap(_event => {
      val header = _event.getHeader.asInstanceOf[EventHeaderV4]
      if(header.getEventType.equals(EventType.QUERY)) {
        val data = _event.getData.asInstanceOf[QueryEventData]
        Some(data.getSql)
      } else {
        reader(binlogReader.readEvent)
      }
    })

    reader(binlogReader.readEvent)
  }
}
