package parser

import com.foundationdb.sql.parser._

import scala.util.{Failure, Success, Try}

class FoundationParser {
  val parser = new SQLParser()

  def toInsertStatement(statementNode: Try[StatementNode]): Try[List[InsertStatement]] = {
    statementNode.map({
      case n: InsertNode =>
        val v = new ResultSetVisitor
        n.accept(v)
        v.asMaps.map(InsertStatement(n.getTargetTableName.toString, _))
    })
  }

  private def cleanedForParsing(s: String): String = s.replaceAll("""\\'""", "\"")

  def parse(statement: String): Try[List[InsertStatement]] = {
    val parsedStatement = cleanedForParsing(statement)
    val t = Try(parser.parseStatement(parsedStatement))

    toInsertStatement(t) match {
      case s @ Success(n: List[InsertStatement]) => s
      case Success(_) => Failure(new RuntimeException("Parsed but not an insert!"))
      case f @ Failure(t: Throwable) => f
    }
  }
}

object FoundationParser {
  def parse(stm: String): Try[List[InsertStatement]] = (new FoundationParser).parse(stm)
}

class ResultSetVisitor extends Visitor {
  var currentValues = List[String]()
  var cols = List[String]()

  override def visit(visitable: Visitable): Visitable = {
    visitable match {
      case node: BooleanConstantNode =>
        currentValues = node.getValue.toString :: currentValues
      case node: NumericConstantNode =>
        currentValues = node.getValue.toString :: currentValues
      case node: CharConstantNode =>
        currentValues = node.getValue.toString :: currentValues
      case _: UntypedNullConstantNode =>
        currentValues = "NULL" :: currentValues
      case n: ColumnReference =>
        cols = n.getColumnName :: cols
      case _ => ()
    }

    visitable
  }

  override def stopTraversal(): Boolean = false

  override def skipChildren(visitable: Visitable): Boolean = false

  override def visitChildrenFirst(visitable: Visitable): Boolean = true

  def asMaps = currentValues.grouped(cols.size).map(cols.zip(_).toMap).toList
}
