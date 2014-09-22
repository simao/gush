package binlog

import parser._

import scala.util.{Failure, Success}

object BinlogEvent {
  def apply(raw_sql: String) = {
    def parser = new CustomInsertParser

    parser(raw_sql) match {
      case Success(m) => { new BinlogEvent(raw_sql, m.tableName,  m.fields) }
      case Failure(t) => throw new Exception(s"Error Parsing: ${raw_sql}: ", t)
    }
  }
}

class BinlogEvent(val raw_sql: String, val tableName: String,
  val fields: Map[String, String]) { }
