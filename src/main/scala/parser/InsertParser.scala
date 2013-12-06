package parser

import scala.util.{Try, Success, Failure}
import com.akiban.sql.parser.SQLParser
import com.akiban.sql.parser.StatementNode
import com.akiban.sql.parser.Visitor
import com.akiban.sql.parser.InsertNode
import com.akiban.sql.parser.Visitable
import com.akiban.sql.parser._
import scala.collection.immutable._

case class InsertStatement(tableName: String, fields: Map[String, String]) {}

class InsertVisitor extends Visitor {
  var values = List[String]()
  var names = List[String]()
  var tableName: Option[String] = None

  def skipChildren(v: Visitable): Boolean = false

  def stopTraversal(): Boolean = false

  def addName(n: String) { names = n :: names }

  def addValue(v: String) { values = v :: values }

  def setTableName(t: String) { tableName = Some(t) }

  def visit(x: Visitable): Visitable = {
    x match {
      case c: CharConstantNode => addValue(c.getString)
      case n: NumericConstantNode => addValue(n.getValue.toString)
      case n: UntypedNullConstantNode => addValue("NULL")
      case cr: ColumnReference => addName(cr.getColumnName)
      case t: TableName => setTableName(t.getTableName)
      case b: BooleanConstantNode => addValue(b.getValue.toString)
      case _:InsertNode => ()
      case _:ResultColumn => ()
      case _:RowResultSetNode => ()
      case _:ResultColumnList => ()
      case _ => throw new Exception("Error parsing: " + x.getClass.getSimpleName)
    }

    x
  }

  def visitChildrenFirst(v: Visitable): Boolean = false

  def fields = (names zip values).toMap

  def getTableName = tableName
}

class InsertParser  {
  def parse(statement: String) = {
  val parser = new SQLParser();
    val v = new InsertVisitor
    val r = Try(parser.parseStatement(statement).accept(v))

    r.map(_ => new InsertStatement(v.getTableName getOrElse "", v.fields))
  }

  // TODO: Well I need to make this work for now
  def cleanForParsing(s: String) = s.replaceAll("""\\'""", "X")

  def apply(statement: String) = {
    parse(cleanForParsing(statement))
  }
}
