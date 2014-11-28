package parser

sealed trait SqlStatement {
  def table: String
  def fields: Map[String, String]
}

case class InsertStatement(table: String, fields: Map[String, String])
  extends SqlStatement

case class UpdateStatement(table: String, fields: Map[String, String])
  extends SqlStatement


