package binlog

import binlog._
import esper._

import com.github.shyiko.mysql.binlog.network.SocketFactory

import java.net.Socket

import com.github.shyiko.mysql.binlog._
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.BinaryLogClient._
import rx.lang.scala.{Observable, Subscription, Observer}

class EventListener(observer: Observer[String]) extends BinaryLogClient.EventListener {
  def onEvent(event: Event) {
    val header = event.getHeader.asInstanceOf[EventHeaderV4]
    if(header.getEventType.equals(EventType.QUERY)) {
      val data = event.getData.asInstanceOf[QueryEventData]
      observer.onNext(data.getSql)
    }
  }
}

class LifecycleListener(observer: Observer[String]) extends BinaryLogClient.LifecycleListener {
  def onConnect(client: BinaryLogClient) = Unit

  def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {
    observer.onError(ex)
  }

  def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {
    observer.onError(ex)
  }

  def onDisconnect(client: BinaryLogClient) {
    observer.onCompleted
  }
}

class BinlogRemoteReader(val host: String, val port: Int, val user: String, val password: String) extends BinlogSqlStream {

  def observableFrom(client: BinaryLogClient) = {
    Observable.create((o: Observer[String]) => {
      val eventListener = new EventListener(o)
      val lifecycleListener = new LifecycleListener(o)
      client.registerEventListener(eventListener)
      client.registerLifecycleListener(lifecycleListener)

      client.connect

      // TODO: Doesnt work with multiple subscribers unless this is executed for each subscriber
      Subscription {
        client.unregisterEventListener(eventListener)
        client.unregisterLifecycleListener(lifecycleListener)
        client.disconnect
      }
    })
  }

  // TODO: Will only connect when the returned observable is subscribed to
  // TODO: I think everytime `subscribe` is called on the obverser, we'll connect again. (Maybe not)
  override def events = {
    val client = new BinaryLogClient(host, port, user, password)

    val observable = observableFrom(client)

    observable
  }
}
