package io.simao.gush.binlog

import io.simao.gush.util.GushConfig
import com.github.shyiko.mysql.binlog._
import com.github.shyiko.mysql.binlog.event._
import com.jcraft.jsch.JSch
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import rx.lang.scala.{Observable, Observer, Subscription}

import scala.util.{Failure, Success, Try}
import scalaz.{-\/, \/-, \/}
import scalaz.syntax.std.option._
import scalaz.syntax.either._

class BinlogEventListener(observer: Observer[String])(implicit val config: GushConfig) extends BinaryLogClient.EventListener with StrictLogging {
  def onEvent(event: Event) {
    val header = event.getHeader.asInstanceOf[EventHeaderV4]
    if(header.getEventType.equals(EventType.QUERY)) {
      val data = event.getData.asInstanceOf[QueryEventData]

      if (ignored_event(data.getSql)) {
        logger.trace(s"Event ignored: ${data.getSql.slice(0, 30)}")
      } else {
        logger.debug(s"Sending binlog event to observer (${data.getSql.slice(0, 30)})")
        observer.onNext(data.getSql)
      }
    }
  }

  // TODO: Should be moved outside somehow
  def ignored_event(sqlStatement: String) = {
    config.ignored_tables.exists { tn => sqlStatement.contains(s"`$tn`") } ||
    config.ignored_prefixes.exists(sqlStatement.startsWith)
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

object BinlogClientBuilder extends LazyLogging {
  def ssh(config: GushConfig): \/[Throwable, BinaryLogClient] = {
    val params = for {
      host <- config.mysqlHost
      port <- config.mysqlPort
      user <- config.mysqlUser
      password <- config.mysqlPassword
      sshAddress <- config.sshTunnelAddress
      sshTunnelUser <- config.sshTunnelUser
    } yield (host, port, user, password, sshAddress, sshTunnelUser)

    params match {
      case Some((host, port, user, password, sshAddress, sshTunnelUser)) ⇒
        Try({
          val jsch = new JSch()
          jsch.addIdentity(System.getProperty("user.home") + "/.ssh/id_rsa")

          val session = jsch.getSession(sshTunnelUser, sshAddress)
          session.setConfig("StrictHostKeyChecking", "no")

          val lport = session.setPortForwardingL(0, host, port)

          session.connect(3000)

          logger.info(s"Forwarding 127.0.0.1:$lport to $host:$port")

          new BinaryLogClient("127.0.0.1", lport, user, password)
        }) match {
          case Success(v) ⇒ v.right
          case Failure(t) ⇒ t.left
        }
      case None ⇒
        new Exception("Not enough parameters to initialize a ssh client").left
    }
  }

  def direct(config: GushConfig): \/[Throwable, BinaryLogClient] = {
    val connection = for {
      host <- config.mysqlHost
      port <- config.mysqlPort
      user <- config.mysqlUser
      password <- config.mysqlPassword
    } yield new BinaryLogClient(host, port, user, password)

    connection.toRightDisjunction(new Exception("Not enough parameters to start a binlog client"))
  }
}

object BinlogRemoteReader extends LazyLogging {
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

  def events(config: GushConfig) = {
    setupClient(config) match {
      case \/-(client) ⇒
        observableFrom(client)
      case -\/(t) ⇒
        throw t
    }
  }

  private def setupClient(config: GushConfig): \/[Throwable, BinaryLogClient] = {
    config.sshTunnelAddress
      .map(x ⇒ BinlogClientBuilder.ssh(_))
      .getOrElse(BinlogClientBuilder.direct(_))
      .apply(config)
  }
}

