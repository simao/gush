package esper

import scala.beans.BeanProperty

trait BinlogEvent

case class BinlogInsertEvent(@BeanProperty tableName: String,
                             @BeanProperty fields: Map[String, String]) extends BinlogEvent {
  def getField(k: String) = fields(k)

  def getAsFloat(k: String) = fields(k).toFloat
}

case class BinlogUpdateEvent(@BeanProperty tableName: String,
                             @BeanProperty updatedFields: Map[String, String],
                             @BeanProperty whereFields: Map[String, String]) extends BinlogEvent
