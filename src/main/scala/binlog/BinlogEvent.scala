package binlog

import esper.{BinlogEvent, BinlogUpdateEvent, BinlogInsertEvent}
import parser._

import scala.util.{Failure, Try}

object BinlogEvent {
  def parseAll(raw_sql: String): Try[List[BinlogEvent]] = {
    FoundationParser
      .parse(raw_sql)
      .map(_.map(parsedStatementToBinlogEvent))
      .recoverWith({
      case t: Throwable ⇒ Failure(new RuntimeException(s"Error Parsing: $raw_sql: ", t))
    })
  }


  def parsedStatementToBinlogEvent(stm: SqlStatement): BinlogEvent = stm match {
      case s: InsertStatement ⇒
        new BinlogInsertEvent(stm.table, stm.fields)
      case s: UpdateStatement ⇒
        new BinlogUpdateEvent(s.table, s.updatedFields, s.target)
  }
}


