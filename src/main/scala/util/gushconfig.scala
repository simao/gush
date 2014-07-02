package util

import java.io.{FileInputStream, File}
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConversions._

class GushConfig(val config: Map[String, Object]) {
  def apply(v: String) = { config(v).toString }

  def ignored_tables = {
    config("ignored_tables").asInstanceOf[java.util.List[String]].toList
  }

  def ignored_prefixes = {
    config("ignored_statements_prefixes").asInstanceOf[java.util.List[String]].toList
  }
}

object GushConfig {
  implicit val default: GushConfig = GushConfig("gush.config.yml")

  def apply(path: String) = {
    val input = new FileInputStream(new File(path))
    val yaml = new Yaml()
    val data = yaml.load(input)

    new GushConfig(data.asInstanceOf[java.util.Map[String, String]].toMap)
  }
}
