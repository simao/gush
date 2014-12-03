package io.simao.gush

import io.simao.gush.parser.{FoundationParser, UpdateStatement}
import org.scalatest.{Tag, FunSuite}

import scala.util.Try

object IgnoreTest extends Tag("IgnoreTest")

class FoundationParserTest extends FunSuite {

  def parseFirst(stm: String) = FoundationParser.parse(stm).map(_.head).get

  test("parses fields in insert object") {
    val stm = "insert INTO `messages` (`approved`, `autoapproved`, `body`, `booking_id`, `checkin_date`, `checkout_date`, `code`, `created_at`, `email_sent`, `first_reply_at`, `guests`, `offer_country_code_iso`, `offer_id`, `read_at`, `recipient_deleted_at`, `recipient_id`, `replies_count`, `reply_to_message_id`, `sender_deleted_at`, `sender_id`, `subject`, `updated_at`) values (1, 0, 'Hallo Herr Buissant\r\nich würde gerne wissen ob wir bei ihnen auch unser auto abstellen können,fahrräder leihen und wie es mit bettwäsche aussieht und was sie noch für fragen haben,\r\nliebe grüße christiane gottschalk', 5299810, '2013-12-20', '2013-12-27', '88DSCQRJ', '2013-11-23 18:20:10', 0, NULL, 3, NULL, NULL, NULL, NULL, 4754165, 0, NULL, NULL, 4995086, 'Buchungsanfrage', '2013-11-23 18:20:10')"
    val record = parseFirst(stm)
    assert(record.fields("approved") === "1")
    assert(record.fields("reply_to_message_id") == "NULL")
  }

  test("ignores comments in the end of the statement") {
    val stm = "INSERT INTO `messages` (`approved`) VALUES (true) /* lol */"

    val record = parseFirst(stm)

    assert(record.fields === Map("approved" -> "true"))
  }

  test("Returns a failure when string could not be parsed") {
    val failure = FoundationParser.parse("invalid statement")
    assert(failure.isFailure)
  }

    test("parses empty fields") {
    val stm = "INSERT INTO `messages` (`approved`) VALUES ('') "
    val record = parseFirst(stm)

    assert(record.fields === Map("approved" -> ""))
  }

  test("idents can be unquoted") {
    val stm = "INSERT INTO geo_locations_offers (geo_location_id, offer_id) VALUES (2427,3143264)"
    val record = parseFirst(stm)

    assert(record.fields === Map("geo_location_id" -> "2427", "offer_id" ->  "3143264"))
  }

  test("multiple rows are ignored") {
    val stm = "INSERT INTO geo_locations_offers (geo_location_id, offer_id) VALUES (2427,3143264), (2427, 3143264)"

    val record = parseFirst(stm)

    assert(record.fields === Map("geo_location_id" -> "2427", "offer_id" ->  "3143264"))
  }

  test("multiple rows are parsed") {
    val stm = "INSERT INTO geo_locations_offers (geo_location_id, offer_id) VALUES ((2427,3143264), (2, 22))"
    val records = FoundationParser.parse(stm).get

    val fields = records.map(_.fields)

    assert(fields.contains(Map("geo_location_id" -> "2427", "offer_id" -> "3143264")))
    assert(fields.contains(Map("geo_location_id" -> "2", "offer_id" ->  "22")))
  }

  test("parses statements with double escaped single quotes") {
    val stm = "INSERT INTO `table_name` (`col_name`) VALUES ('\\'value\\'')"
    val record = parseFirst(stm)

    assert(record.fields("col_name") === "\"value\"")
  }

  test("fails ON DUPLICATE KEY UPDATE") {
    val stm = "INSERT INTO vegetables (name) VALUES ('cenouras') ON DUPLICATE KEY name = 'batatas'"
    val record = FoundationParser.parse(stm)

    assert(record.isFailure)
  }

  test("parse an update") {
    val stm = "UPDATE `reviews` SET `reviews`.`offer_is_online` = 0, " +
      "`reviews`.`batatas` = 22, " +
      "`reviews`.`cenouras` = NULL " +
      "WHERE " +
      "`reviews`.`batatas` = NULL AND " +
      "`reviews`.`type` IN ('HostReview', 'GuestReview') AND " +
      "`reviews`.`recipient_id` = 1528143 /*application*/"

    val parsedStatement: Try[List[UpdateStatement]] = (new FoundationParser).parse(stm)

    val record = parsedStatement.get.head

    assert(record.table === "reviews")
    assert(record.fields("batatas") === "22")
    assert(record.fields("cenouras") === "NULL")
    assert(record.fields("offer_is_online") === "0")
    assert(record.target("type") === "HostReview, GuestReview")
    assert(record.target("recipient_id") === "1528143")
  }

  test("parse COALESCE in UPDATE") {
    val stm = "UPDATE `offers` SET `guest_reviews_count` = COALESCE(`guest_reviews_count`, 0) + 1 WHERE `offers`.`id` = 3533930"

    val parsedStatement: Try[List[UpdateStatement]] = (new FoundationParser).parse(stm)

    val record = parsedStatement.get.head

    assert(record.table === "offers")
    assert(record.updatedFields("guest_reviews_count") === "COALESCE(guest_reviews_count,0)+1")
  }
}
