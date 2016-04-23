package io.simao.gush.parser

import com.foundationdb.sql.parser._

import scala.util.{Success, Failure, Try}

class FoundationParser {
  val parser = new SQLParser()

  def toSqlStatement(statementNode: Try[StatementNode]): Try[List[SqlStatement]] = {
    statementNode.flatMap({
      case n: UpdateNode ⇒
        val v = new UpdateNodeVisitor
        n.accept(v)
        Success(List(v.parsedUpdate))

      case n: InsertNode ⇒
        val v = new InsertNodeVisitor
        n.accept(v)
        Success(v.asMaps.map(InsertStatement(n.getTargetTableName.toString, _)))

      case n ⇒
        Failure(new RuntimeException(s"Unknown parsed statement: ${n.toString}"))
    })
  }

  // Getting weird escaping from mysql-io.simao.gush.binlog
  private def cleanedForParsing(s: String): String = s.replaceAll("""\\'""", "\"")

  def parse[T <: SqlStatement](statement: String): Try[List[T]] = {
    val parsedStatement = cleanedForParsing(statement)
    val t = Try(parser.parseStatement(parsedStatement))

    // ? Why asInstanceof?
    toSqlStatement(t).asInstanceOf[Try[List[T]]]
  }
}

object FoundationParser {
  def parse(stm: String): Try[List[SqlStatement]] = (new FoundationParser).parse(stm)
}
