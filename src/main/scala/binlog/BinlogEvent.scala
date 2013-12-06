package binlog

import parser._
import scala.util.{Success, Failure}
import com.espertech.esper.client.{EventBean, UpdateListener, EPServiceProvider}
import scala.reflect.BeanProperty

case class BinlogStreamEvent(@BeanProperty val tableName: String,
                             @BeanProperty val fields: Map[String, String]) {
  def getField(k: String) = fields(k)

  def getAsFloat(k: String) = fields(k).toFloat
}
 
object BinlogEvent {
  def apply(raw_sql: String) = {
    def parser = new CustomInsertParser

    parser(raw_sql) match {
      case Success(m) => { new BinlogEvent(raw_sql, m.tableName,  m.fields) }
      case Failure(t) => throw t
    }
  }
}

class BinlogEvent(val raw_sql: String, val tableName: String,
  val fields: Map[String, String]) { }
