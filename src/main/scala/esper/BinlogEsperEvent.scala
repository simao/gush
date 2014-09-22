package esper

import scala.beans.BeanProperty

case class BinlogEsperEvent(@BeanProperty val tableName: String,
                             @BeanProperty val fields: Map[String, String]) {
  def getField(k: String) = fields(k)

  def getAsFloat(k: String) = fields(k).toFloat
}
