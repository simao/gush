package binlog

import parser._

import scala.util.{Failure, Success}

object BinlogEvent {
  // TODO: Should return a Try
  def parseAll(raw_sql: String): List[BinlogEvent] = {
      FoundationParser.parse(raw_sql) match {
      case Success(m) => m.map({x => new BinlogEvent(raw_sql, x.table,  x.fields)})
      case Failure(t) =>
        throw new Exception(s"Error Parsing: $raw_sql: ", t)
    }
  }
}

case class BinlogEvent(raw_sql: String, tableName: String, fields: Map[String, String])
