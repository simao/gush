package io.simao.gush.parser

import com.foundationdb.sql.parser.{ColumnReference, ConstantNode, Visitable, Visitor}

class InsertNodeVisitor extends Visitor {
  var currentValues = List[String]()
  var cols = List[String]()

  override def visit(node: Visitable): Visitable = node match {
    case n: ConstantNode ⇒
      val newValue = Option(n.getValue).getOrElse("NULL").toString
      currentValues = newValue :: currentValues
      node
    case n: ColumnReference ⇒
      cols = n.getColumnName :: cols
      node
    case _ =>
      node
  }

  override def stopTraversal(): Boolean = false

  override def skipChildren(visitable: Visitable): Boolean = false

  override def visitChildrenFirst(visitable: Visitable): Boolean = true

  def asMaps = currentValues.grouped(cols.size).map(cols.zip(_).toMap).toList
}

