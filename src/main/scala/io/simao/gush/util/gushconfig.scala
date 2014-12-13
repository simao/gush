package io.simao.gush.util

import java.io.{File, FileInputStream}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions._

class GushConfig(val config: Map[String, Object]) {
  def mysqlPort: Option[Int] = getProperty("mysql_port").map(_.toInt)

  def mysqlHost = getProperty("mysql_host")

  def mysqlUser = getProperty("mysql_user")

  def mysqlPassword = getProperty("mysql_password")

  def statsDHost: String = getProperty("statsd_host").getOrElse("localhost")

  def sshTunnelAddress = getProperty("ssh_tunnel_host")

  def sshTunnelUser = getProperty("ssh_tunnel_user").orElse(Some("gush"))

  def ignored_tables = getListProperty("ignored_tables")

  def ignored_prefixes = getListProperty("ignored_statements_prefixes")

  def getProperty(key: String): Option[String] = config.lift(key).map(_.toString)

  def getListProperty(key: String): List[String] = {
    config.lift(key)
    .map(_.asInstanceOf[java.util.List[String]])
    .map(_.toList)
    .getOrElse(List())
  }
}

object GushConfig {
  implicit val default: GushConfig = {
    defaultConfigFile
      .map(_.getAbsolutePath)
      .map(GushConfig(_))
      .getOrElse(new GushConfig(Map()))
  }

  def defaultConfigFile: Option[File] = {
    val f = new File("gush.config.yml")
    if(f.exists() && f.isFile)
      Some(f)
    else
      None
  }

  def apply(path: String) = {
    val input = new FileInputStream(new File(path))
    val yaml = new Yaml()
    val data = yaml.load(input)

    new GushConfig(data.asInstanceOf[java.util.Map[String, String]].toMap)
  }
}
