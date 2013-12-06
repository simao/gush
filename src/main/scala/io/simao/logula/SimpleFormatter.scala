package io.simao.logula

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.EnhancedPatternLayout

class SimpleFormatter(format: String) extends EnhancedPatternLayout(format) {

  override def format(event: LoggingEvent) = {
    val msg = new StringBuilder
    msg.append("[" + event.getLevel.toString.charAt(0) + "]")
    msg.append(super.format(event))
    if (event.getThrowableInformation != null) {
      for (line <- event.getThrowableInformation.getThrowableStrRep) {
        msg.append("! ")
        msg.append(line)
        msg.append("\n")
      }
    }
    msg.toString
  }

  override def ignoresThrowable = false
}
