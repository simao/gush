package io.simao.gush

import io.simao.util.KestrelObj._
import org.scalatest.FunSuite

// TODO: TEst properly
class UtilTest extends FunSuite {
  test("kestrel") {
    assert(1.tap { v ⇒ println(v) } === 1)
  }
}
