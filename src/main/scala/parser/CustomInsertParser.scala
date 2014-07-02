package parser

import scala.util.{Try, Success, Failure}
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._

case class InsertStatement(tableName: String, fields: Map[String, String]) {}

class CustomInsertParser extends JavaTokenParsers {
  def quotedStr = "'" ~> """[^']*""".r <~ "'"

  def colValue = (NULL | boolean | decimalNumber | negativeDecimalNumber | quotedStr)

  def boolean = ("true" | "false")

  def NULL = literal("NULL")

  def negativeDecimalNumber = "-" ~> decimalNumber ^^ { case d => "-" + d }

  def colList = "(" ~> rep1sep(sqlId, ",") <~ ")"

  def valList = "(" ~> rep1sep(colValue, ",") <~ ")"

  def quotedId = ( "`" ~> ident <~ "`" )

  def sqlId = quotedId | ident

  def values = """(?i)VALUES""".r ~> rep1sep(valList, ",")

  def insertInto = """(?i)INSERT INTO""".r

  def comment = "/*" ~ """.+\*/""".r

  def insert = (insertInto ~> sqlId ~ colList ~ values <~ comment.?)

  def insertSingle:Parser[InsertStatement] = {
    insert ^^ {
      case tableName ~ cols ~ vals => {
        val insertVals = vals(0) // We can return a Parser.Failure if vals is empty (?)
        assert(cols.size == insertVals.size)

        val fields = (cols zip insertVals).toMap
        InsertStatement(tableName, fields)
      }
    }
  }

  def insertMulti: Parser[List[InsertStatement]] = {
    insert ^^ {
      case tableName ~ cols ~ vals => {
        vals.map(v => {
          assert(cols.size == v.size)

          val fields = (cols zip v).toMap
          InsertStatement(tableName, fields)
        })
      }
    }
  }

  // TODO: Well I need to make this work for now
  private def cleanForParsing(s: String) = s.replaceAll("""\\'""", "X")

  def apply(statement: String): Try[InsertStatement] = {
    parseAll(insertSingle, cleanForParsing(statement)) match {
      case Success(result, _) => scala.util.Success(result)
      case NoSuccess(msg, _) => scala.util.Failure(new Exception(msg))
    }
  }
}
