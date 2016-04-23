package io.simao.gush.binlog

import akka.actor.{ActorLogging, Actor, Props}
import akka.stream.actor.ActorPublisher
import io.simao.gush.util.GushConfig
import com.github.shyiko.mysql.binlog._
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}

import scala.annotation.tailrec
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


object BinlogRemoteReaderActor extends LazyLogging {
  case class Publish(sql: String)
  case object Connect

  def props(client: BinaryLogClient): Props = {
    Props[BinlogRemoteReaderActor](new BinlogRemoteReaderActor(client))
  }

  def client(config: GushConfig): BinaryLogClient = {
    BinlogClientBuilder.direct(config) match {
      case Success(client) ⇒
        client
      case Failure(t) ⇒
        throw t
    }
  }
}

class BinlogRemoteReaderActor(val client: BinaryLogClient) extends ActorPublisher[String] with StrictLogging
with ActorLogging
{
  import BinlogRemoteReaderActor._
  import akka.stream.actor.ActorPublisherMessage._

  val MaxBufferSize = 100
  var buf = Vector.empty[String]

  override def preStart(): Unit = {
    val eventListener = new BinlogEventListener(s ⇒ self ! Publish(s))
    val lifecycleListener = new LifecycleListener(handleClientError, onComplete)
    client.registerEventListener(eventListener)
    client.registerLifecycleListener(lifecycleListener)
    self ! Connect
  }

  override def postRestart(reason: Throwable): Unit = {
    Try(client.disconnect()).failed map (logger.warn("Could not disconnect client", _))
    client.connect()
  }

  override def receive: Actor.Receive = {
    case Connect ⇒
      client.connect()
    case Publish(sql) ⇒
      addToBuffer(sql)
    case Request(n) ⇒
      deliverBuf()
    case Cancel ⇒
      logger.warn("Received Cancel, stopping")
      context.stop(self)
  }

  protected def addToBuffer(sql: String): Unit = {
    buf :+= sql
    deliverBuf()
  }

  protected def handleClientError(throwable: Throwable): Unit = {
    logger.error("Error with client", throwable)
    onError(throwable)
  }

  @tailrec private final def deliverBuf(): Unit = {
    if (isActive && (totalDemand > 0) && buf.nonEmpty) {
      val maxDemand = Math.max(Int.MaxValue, totalDemand).toInt
      val (use, keep) = buf.splitAt(maxDemand)
      buf = keep
      use foreach onNext
      deliverBuf()
    }
  }
}
