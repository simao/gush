package io.simao.gush


import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.scalalogging.StrictLogging
import io.simao.gush.akka_streams.BinlogToAkkaStreams
import io.simao.gush.binlog.{BinlogEvent, BinlogRemoteReader}
import io.simao.gush.util.{StatsdSender, GushConfig}

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.Try


class Gush(implicit config: GushConfig) extends StatsdSender with StrictLogging {

  val defaultSink: Sink[BinlogEvent, Future[Unit]] = Sink.foreach(println)

  @tailrec final def startBinlogLoop()(implicit materializer: Materializer): Unit = {
    val eventSource = BinlogRemoteReader.events(config)

    Try(new BinlogToAkkaStreams(eventSource, config).startSending(defaultSink)) recover {
      case ex =>
        statsd.increment("gush.exceptions.loop")
        logger.error("Error occurred: ", ex)
    }

    startBinlogLoop()
  }
}

object GushApp extends App with StatsdSender {
  implicit val system = ActorSystem("reactive-gush")
  implicit val materializer = ActorMaterializer()

  (new Gush).startBinlogLoop()
}
