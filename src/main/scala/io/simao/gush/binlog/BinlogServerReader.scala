package io.simao.gush.binlog

import io.simao.gush.util.GushConfig
import com.github.shyiko.mysql.binlog._
import com.github.shyiko.mysql.binlog.event._
import com.jcraft.jsch.JSch
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import rx.lang.scala.{Observable, Observer, Subscription}

class BinlogEventListener(observer: Observer[String])(implicit val config: GushConfig) extends BinaryLogClient.EventListener with StrictLogging {
  def onEvent(event: Event) {
    val header = event.getHeader.asInstanceOf[EventHeaderV4]
    if(header.getEventType.equals(EventType.QUERY)) {
      val data = event.getData.asInstanceOf[QueryEventData]

      if (ignored_event(data.getSql)) {
        logger.trace(s"Event ignored: ${data.getSql.slice(0, 30)}")
      } else {
        logger.debug(s"Sending io.simao.gush.binlog event to observer (${data.getSql.slice(0, 30)})")
        observer.onNext(data.getSql)
      }
    }
  }

  def ignored_event(sqlStatement: String) = {
    config.ignored_tables.exists { tn => sqlStatement.contains(s"`$tn`") } ||
    config.ignored_prefixes.exists { p => sqlStatement.startsWith(p) }
  }
}

class LifecycleListener(observer: Observer[String]) extends BinaryLogClient.LifecycleListener  with StrictLogging {
  def onConnect(client: BinaryLogClient) {
    logger.info(s"Connected to mysql master. Filename: ${client.getBinlogFilename}, position: ${client.getBinlogPosition}")
  }

  def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {
    observer.onError(ex)
  }

  def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {
    observer.onError(ex)
  }

  def onDisconnect(client: BinaryLogClient) {
    logger.warn("mysql binlog disconnected")
    observer.onCompleted()
  }
}

class BinlogRemoteReader(config: GushConfig) extends LazyLogging {

  def observableFrom(client: BinaryLogClient) = {
    Observable.create((o: Observer[String]) => {
      val eventListener = new BinlogEventListener(o)
      val lifecycleListener = new LifecycleListener(o)
      client.registerEventListener(eventListener)
      client.registerLifecycleListener(lifecycleListener)

      client.connect()

      Subscription {
        client.unregisterEventListener(eventListener)
        client.unregisterLifecycleListener(lifecycleListener)
        client.disconnect()
      }
    })
  }

  def setupTunnelledClient(): Option[BinaryLogClient] = {
    for {
      host <- config.mysqlHost
      port <- config.mysqlPort
      user <- config.mysqlUser
      password <- config.mysqlPassword
      sshAddress <- config.sshTunnelAddress
      sshTunnelUser <- config.sshTunnelUser
    } yield {
      val jsch = new JSch()
      jsch.addIdentity(System.getProperty("user.home") + "/.ssh/id_rsa")

      val session = jsch.getSession(sshTunnelUser, sshAddress)
      session.setConfig("StrictHostKeyChecking", "no")

      val lport = session.setPortForwardingL(0, host, port)

      session.connect(3000)

      logger.info(s"Forwarding 127.0.0.1:$lport to $host:$port")

      new BinaryLogClient("127.0.0.1", lport, user, password)
    }
  }

  def setupSimpleClient(): Option[BinaryLogClient] = {
    for {
      host <- config.mysqlHost
      port <- config.mysqlPort
      user <- config.mysqlUser
      password <- config.mysqlPassword
    } yield new BinaryLogClient(host, port, user, password)
  }

  def setupClient(): Option[BinaryLogClient] = {
    config.sshTunnelAddress
      .map(_ => setupTunnelledClient())
      .getOrElse(setupClient())
  }

  def events = {
    setupClient() match {
      case Some(client) =>
        observableFrom(client)
      case None =>
        throw new Exception("Could not initialize binlog client with current config file")
    }
  }
}
