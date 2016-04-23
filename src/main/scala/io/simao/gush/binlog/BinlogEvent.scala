package io.simao.gush.binlog

import io.simao.gush.parser._

import scala.beans.BeanProperty
import scala.util.{Failure, Try}

class BinlogEventParseError(val msg: String, val t: Throwable) extends Exception(msg, t)

object BinlogEvent {
  def parseAll(raw_sql: String): Try[List[BinlogEvent]] = {
    FoundationParser
      .parse(raw_sql)
      .map(_.map(parsedStatementToBinlogEvent))
      .recoverWith {
        case t ⇒ Failure(new BinlogEventParseError(s"Error Parsing: $raw_sql: ", t))
      }
  }

  def parsedStatementToBinlogEvent(stm: SqlStatement): BinlogEvent = stm match {
      case s: InsertStatement ⇒
        new BinlogInsertEvent(stm.table, stm.fields)
      case s: UpdateStatement ⇒
        new BinlogUpdateEvent(s.table, s.updatedFields, s.target)
  }
}

sealed trait BinlogEvent {
  def tableName: String
}

case class BinlogInsertEvent(tableName: String,
                             fields: Map[String, String]) extends BinlogEvent {
  def getField(k: String) = fields(k)

  def getAsFloat(k: String) = fields(k).toFloat
}

case class BinlogUpdateEvent(tableName: String,
                             updatedFields: Map[String, String],
                             whereFields: Map[String, String]) extends BinlogEvent

