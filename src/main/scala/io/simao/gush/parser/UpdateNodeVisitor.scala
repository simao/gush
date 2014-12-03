package io.simao.gush.parser

import com.foundationdb.sql.parser._
import io.simao.gush.parser.SqlStatement.FieldMap

class UpdatedFieldsVisitor extends Visitor {
  var fields = Map[String, String]()

  override def visit(node: Visitable): Visitable = node match {
    case n: ResultColumn ⇒
      val expVisitor = new ExpressionVisitor
      n.getExpression.accept(expVisitor)
      val expression = expVisitor.asString
      val ref = n.getReference.getColumnName
      fields = fields + (ref → expression)
      node

      case _ ⇒
        node
  }

  override def stopTraversal(): Boolean = false

  override def skipChildren(node: Visitable): Boolean = false

  override def visitChildrenFirst(node: Visitable): Boolean = false

  def asMap = this.fields
}

class WhereFieldsVisitor extends Visitor {
  var fields = Map[String, String]()
  var currentColName: Option[String] = None

  override def visit(node: Visitable): Visitable = {
    node match {
      case n: ConstantNode ⇒
        val colName = currentColName.get
        val v = Option(n.getValue).map(_.toString).getOrElse("NULL")
        val value = newRefvalue(colName, v, fields)
        fields = fields + (colName → value)

      case n: ColumnReference ⇒
        currentColName = Some(n.getColumnName)

      case _ ⇒ ()
    }

    node
  }

  override def stopTraversal(): Boolean = false

  override def skipChildren(node: Visitable): Boolean = false

  override def visitChildrenFirst(node: Visitable): Boolean = false

  def asMap = this.fields

  private def newRefvalue(key: String, value: String, currentValues: Map[String, String]): String = {
    fields.lift(key).map(_ + ", " + value).getOrElse(value)
  }
}

class ExpressionVisitor extends Visitor {
  var expressionRepr = new StringBuilder

  override def visit(node: Visitable): Visitable = node match {
    case n: CoalesceFunctionNode ⇒
      expressionRepr ++= n.getFunctionName + "("

      for(i ← 0 until n.getArgumentsList.size()) {
        n.getArgumentsList.get(i).accept(this)
        if(i != n.getArgumentsList.size()-1)
          expressionRepr ++= ","
      }

      expressionRepr ++= ")"
      node

    case n: BinaryOperatorNode ⇒
      n.getLeftOperand.accept(this)
      expressionRepr ++= n.getOperator
      n.getRightOperand.accept(this)
      node

    case n: ConstantNode ⇒
      expressionRepr ++= Option(n.getValue).map(_.toString).getOrElse("NULL")
      node

    case n: ColumnReference ⇒
      expressionRepr ++= n.getColumnName
      node

    case n: ValueNodeList ⇒
      node
  }

  def asString = expressionRepr.toString()

  override def stopTraversal(): Boolean = false

  override def skipChildren(node: Visitable): Boolean = node match {
    case _: CoalesceFunctionNode | _: BinaryOperatorNode ⇒ true
    case _ ⇒ false
  }

  override def visitChildrenFirst(node: Visitable): Boolean = false
}

class UpdateNodeVisitor extends Visitor {
  var whereFields: FieldMap = Map[String, String]()
  var updatedFields = Map[String, String]()
  var tableName: Option[String] = None

  override def visit(node: Visitable): Visitable = node match {
    case n: UpdateNode ⇒
      tableName = Some(n.getTargetTableName.toString)
      node

    case n: SelectNode ⇒
      val whereVisitor = new WhereFieldsVisitor
      val resultListVisitor = new UpdatedFieldsVisitor

      n.getResultColumns.accept(resultListVisitor)
      n.getWhereClause.accept(whereVisitor)

      whereFields = whereFields ++ whereVisitor.asMap
      updatedFields = updatedFields ++ resultListVisitor.asMap

      node
    case _ ⇒ ()
      node
  }

  override def stopTraversal(): Boolean = false

  override def skipChildren(node: Visitable): Boolean = false

  override def visitChildrenFirst(node: Visitable): Boolean = false

  def parsedUpdate: UpdateStatement = UpdateStatement(tableName.getOrElse("<Gush.UNKNOWN_TABLE>"), updatedFields, whereFields)
}



