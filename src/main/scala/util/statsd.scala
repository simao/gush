package util

import java.net.{DatagramPacket, InetAddress, DatagramSocket}

class Statsd {
  val socket = new DatagramSocket
  val statsd_address = InetAddress.getByName("wimdu-dev04")

  def gauge(key: String, value: Double) = {
    send(s"${key}:${value}|g")
  }

  def increment(key: String, incr: Int = 1) = {
    send(s"${key}:${incr}|c")
  }

  def send(metric: String) = {
    val buf = metric.getBytes
    val packet = new DatagramPacket(buf, buf.length, statsd_address, 8125)
    socket.send(packet)
  }
}

trait StatsdSender {
  val statsd = new Statsd
}
