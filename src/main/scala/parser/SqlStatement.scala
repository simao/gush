package parser

import parser.SqlStatement.FieldMap

sealed abstract class SqlStatement {
  def table: String
  def fields: FieldMap
}

object SqlStatement {
  type FieldMap = Map[String, String]
}

case class InsertStatement(table: String, fields: FieldMap)
  extends SqlStatement

case class UpdateStatement(table: String, updatedFields: FieldMap, target: FieldMap)
  extends SqlStatement {
  def fields = updatedFields
}


