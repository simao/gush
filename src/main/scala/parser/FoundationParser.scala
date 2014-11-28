package parser

import com.foundationdb.sql.parser._

import scala.util.{Failure, Success, Try}

class SelectNodeVisitor extends Visitor {
  var fields = Map[String, String]()
  var currentFieldName: Option[String] = None

  override def visit(node: Visitable): Visitable = {
    node match {
      case n: ConstantNode ⇒
        fields = fields +
          (currentFieldName.get →  s"${n.getValue.toString}")
        node
      case n: ResultColumn ⇒
        currentFieldName = Some(n.getName)
        node
      case _ ⇒
        node
    }
  }

  override def stopTraversal(): Boolean = false

  override def skipChildren(node: Visitable): Boolean = false

  override def visitChildrenFirst(node: Visitable): Boolean = false

  def asMap = this.fields
}

class FromBaseVisitor extends Visitor {
  var fields = Map[String, String]()
  var currentFieldName: Option[String] = None

  override def visit(node: Visitable): Visitable = {
    node match {
      case n: ConstantNode ⇒
        fields = fields + (currentFieldName.get → n.getValue.toString)

      case n: ResultColumn ⇒
        currentFieldName = Some(n.getColumnName)

      case _ ⇒ ()
    }

    node
  }

  override def stopTraversal(): Boolean = false

  override def skipChildren(node: Visitable): Boolean = false

  override def visitChildrenFirst(node: Visitable): Boolean = false

  def asMap = this.fields
}

class UpdateParserVisitor extends Visitor {
  var selector = Map[String, String]()
  var fields = Map[String, String]()

  override def visit(node: Visitable): Visitable = {
    node match {
//      case n: FromBaseTable ⇒
//        val v = new FromBaseVisitor
//        node.accept(v)
//
//        fields = v.asMap
//
//        print("Fields => ")
//        println(fields)

      case n: SelectNode ⇒
        val v = new SelectNodeVisitor
        node.accept(v)

        selector = selector ++ v.asMap

        print("SELECTORS => ")
        println(selector)

      case n ⇒
    }

    println(">>>>>>>>>>>")
    print(node.toString)
    println(node.getClass.toString)
    println("<<<<<<<<<<<")

    node
  }

  override def stopTraversal(): Boolean = false

  override def skipChildren(node: Visitable): Boolean = false

  override def visitChildrenFirst(node: Visitable): Boolean = false
}

class FoundationParser {
  val parser = new SQLParser()

  def toInsertStatement(statementNode: Try[StatementNode]): Try[List[SqlStatement]] = {
    statementNode.map({
      case n: UpdateNode ⇒
        val v = new UpdateParserVisitor
        n.accept(v)
        // v.asMaps.map(UpdateStatement(n.getTargetTableName.toString, _))
        List(UpdateStatement("", Map()))

      case n: InsertNode =>
        val v = new ResultSetVisitor
        n.accept(v)
        v.asMaps.map(InsertStatement(n.getTargetTableName.toString, _))
    })
  }

  private def cleanedForParsing(s: String): String = s.replaceAll("""\\'""", "\"")

  def parse(statement: String): Try[List[SqlStatement]] = {
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
  def parse(stm: String): Try[List[SqlStatement]] = (new FoundationParser).parse(stm)
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
