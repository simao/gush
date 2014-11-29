package binlog

import parser._

import scala.util.{Failure, Try}

object BinlogEvent {
  def parseAll(raw_sql: String): Try[List[BinlogEvent]] = {
    FoundationParser
      .parse(raw_sql)
      .map(_.map(stm ⇒ new BinlogEvent(raw_sql, stm.table, stm.fields)))
      .recoverWith({
      case t: Throwable ⇒ Failure(new RuntimeException(s"Error Parsing: $raw_sql: ", t))
    })
  }
}

case class BinlogEvent(raw_sql: String, tableName: String, fields: Map[String, String])
