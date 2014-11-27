import org.scalatest.FunSuite
import parser.CustomInsertParser

class CustomInsertParserTest extends FunSuite {
 
  test("parses fields in insert object") {
    val stm = "insert INTO `messages` (`approved`, `autoapproved`, `body`, `booking_id`, `checkin_date`, `checkout_date`, `code`, `created_at`, `email_sent`, `first_reply_at`, `guests`, `offer_country_code_iso`, `offer_id`, `read_at`, `recipient_deleted_at`, `recipient_id`, `replies_count`, `reply_to_message_id`, `sender_deleted_at`, `sender_id`, `subject`, `updated_at`) values (1, 0, 'Hallo Herr Buissant\r\nich würde gerne wissen ob wir bei ihnen auch unser auto abstellen können,fahrräder leihen und wie es mit bettwäsche aussieht und was sie noch für fragen haben,\r\nliebe grüße christiane gottschalk', 5299810, '2013-12-20', '2013-12-27', '88DSCQRJ', '2013-11-23 18:20:10', 0, NULL, 3, NULL, NULL, NULL, NULL, 4754165, 0, NULL, NULL, 4995086, 'Buchungsanfrage', '2013-11-23 18:20:10')"

    val parser = new CustomInsertParser
    val insert = parser(stm).get

    assert(insert.tableName === "messages")
    assert(insert.fields("approved") === "1")
    assert(insert.fields("created_at") === "2013-11-23 18:20:10")
    assert(insert.fields("read_at") === "NULL")
  }

  test("ignores comments in the end of the statement") {
    val stm = "INSERT INTO `messages` (`approved`) VALUES (true) /* lol */"

    val r = (new CustomInsertParser)(stm)

    assert(r.get.fields === Map("approved" -> "true"))
  }

  test("Returns a failure when string could not be parsed") {
    val failure = (new CustomInsertParser)("invalid statement")
    assert(failure.isFailure)
  }

  test("parses a list of quoted ids") {
    val parser = new CustomInsertParser

    val r = parser.parseAll(parser.colList, "(`messages`)")

    assert(r.get === List("messages"))

    val r2 = parser.parseAll(parser.colList, "(`messages`, `deletedat`)")
    assert(r2.get === List("messages", "deletedat"))
  }

  test("parses colValue") {
    val parser = new CustomInsertParser

    assert(parser.parseAll(parser.colValue, "1").get === "1")
    assert(parser.parseAll(parser.colValue, "true").get === "true")
    assert(parser.parseAll(parser.colValue, "'hallo'").get === "hallo")
    assert(parser.parseAll(parser.colValue, "NULL").get === "NULL")
  }

  test("parses value list") {
    val valList = "(true, 1,2.0, 'Hallo Herr Buissant\r\nich würde gerne wissen')"

    val parser = new CustomInsertParser

    val r = parser.parseAll(parser.valList, valList)

    assert(r.get === List("true", "1", "2.0", "Hallo Herr Buissant\r\nich würde gerne wissen"))
  }

  test("parses empty fields") {
    val stm = "INSERT INTO `messages` (`approved`) VALUES ('') "
    val r = (new CustomInsertParser)(stm)

    assert(r.get.fields === Map("approved" -> ""))
  }

  test("negative numbers") {
    val s = "(40.4504355, -3.6700498000000152)"

    val parser = new CustomInsertParser

    val r = parser.parseAll(parser.valList, s)

    assert(r.get === List("40.4504355", "-3.6700498000000152"))
  }

  test("idents can be unquoted") {
    val s = "INSERT INTO geo_locations_offers (geo_location_id, offer_id) VALUES (2427,3143264)"

    val parser = new CustomInsertParser

    val r = parser.parseAll(parser.insertSingle, s)

    assert(r.get.fields === Map("geo_location_id" -> "2427", "offer_id" ->  "3143264"))
  }

  test("multiple rows are ignored") {
    val s = "INSERT INTO geo_locations_offers (geo_location_id, offer_id) VALUES (2427,3143264), (2427, 3143264)"

    val parser = new CustomInsertParser

    val r = parser.parseAll(parser.insertSingle, s)

    assert(r.get.fields === Map("geo_location_id" -> "2427", "offer_id" ->  "3143264"))
  }

  test("multiple rows are parsed") {
    val s = "INSERT INTO geo_locations_offers (geo_location_id, offer_id) VALUES (2427,3143264), (2, 22)"

    val parser = new CustomInsertParser

    val r = parser.parseAll(parser.insertMulti, s)

    assert(r.get(0).fields === Map("geo_location_id" -> "2427", "offer_id" ->  "3143264"))
    assert(r.get(1).fields === Map("geo_location_id" -> "2", "offer_id" ->  "22"))
  }
}
