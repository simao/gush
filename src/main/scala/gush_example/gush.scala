package gush_example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.StrictLogging
import io.simao.gush.SqlToBinlog
import io.simao.gush.binlog.{BinlogEvent, BinlogEventParseError, BinlogRemoteReaderActor}
import io.simao.gush.util.GushConfig

class Gush(implicit config: GushConfig) extends StrictLogging {
  val decider: Supervision.Decider = {
    case t: BinlogEventParseError ⇒
      logger.error("PARSE ERROR", t)
      Supervision.Resume
    case _ ⇒ Supervision.Stop
  }

  implicit val system = ActorSystem("reactive-gush")
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  val metricsSink: Sink[BinlogEvent, NotUsed] = GushCEP.allSinks()

  def startBinlog(): Unit = {
    val client = BinlogRemoteReaderActor.client(config)
    val eventSource = Source.actorPublisher[String](BinlogRemoteReaderActor.props(client))

    new SqlToBinlog(eventSource, config).startSending(metricsSink)
  }
}

object GushApp extends App {

  (new Gush).startBinlog()
}
