package io.simao.gush.util

import java.net.{DatagramPacket, InetAddress, DatagramSocket}

import com.typesafe.scalalogging.StrictLogging

class Statsd(implicit val config: GushConfig) extends StrictLogging {
  val socket = new DatagramSocket
  val statsd_address = InetAddress.getByName(config.statsDHost.get)
  val statsd_port = 8125

  def gauge(key: String, value: Double) = {
    send(s"$key:${value}|g")
  }

  def increment(key: String, incr: Int = 1) = {
    send(s"${key}:${incr}|c")
  }

  def send(metric: String) = {
    logger.debug(s"statsd: ${metric}")

    val buf = metric.getBytes
    val packet = new DatagramPacket(buf, buf.length, statsd_address, statsd_port)
    socket.send(packet)
  }
}

trait StatsdSender {
  val statsd = new Statsd
}
