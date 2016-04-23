package io.simao.gush.binlog

import io.simao.gush.util.GushConfig
import com.github.shyiko.mysql.binlog._
import com.github.shyiko.mysql.binlog.event._
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}

import scala.util.{Failure, Success, Try}

class BinlogEventListener(onNext: String ⇒ Unit)(implicit val config: GushConfig) extends BinaryLogClient.EventListener with StrictLogging {
  def onEvent(event: Event) {
    val header = event.getHeader.asInstanceOf[EventHeaderV4]
    if (header.getEventType.equals(EventType.QUERY)) {
      val data = event.getData.asInstanceOf[QueryEventData]
      logger.debug(s"Sending binlog event to observer (${data.getSql.slice(0, 30)})")
      onNext(data.getSql)
    }
  }
}

class LifecycleListener(onError: Exception ⇒ Unit, onComplete: ⇒ Unit) extends BinaryLogClient.LifecycleListener  with StrictLogging {
  def onConnect(client: BinaryLogClient) {
    logger.info(s"Connected to mysql master. Filename: ${client.getBinlogFilename}, position: ${client.getBinlogPosition}")
  }

  def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {
    onError(ex)
  }

  def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {
    onError(ex)
  }

  def onDisconnect(client: BinaryLogClient) {
    logger.warn("mysql binlog disconnected")
    onComplete
  }
}

object BinlogClientBuilder extends LazyLogging {
  def direct(config: GushConfig): Try[BinaryLogClient] = {
    val connection = for {
      host <- config.mysqlHost
      port <- config.mysqlPort
      user <- config.mysqlUser
      password <- config.mysqlPassword
    } yield new BinaryLogClient(host, port, user, password)

    connection match {
      case Some(c) ⇒ Success(c)
      case None ⇒ Failure(new Exception("Not enough parameters to start a binlog client"))
    }
  }
}




