import org.scalatest.FunSuite
import parser.InsertParser

class InsertParserTest extends FunSuite {
  test("parses fields in insert object") {
    val stm = """insert INTO `messages` (`approved`, `autoapproved`, `body`, `booking_id`, `checkin_date`, `checkout_date`, `code`, `created_at`, `email_sent`, `first_reply_at`, `guests`, `offer_country_code_iso`, `offer_id`, `read_at`, `recipient_deleted_at`, `recipient_id`, `replies_count`, `reply_to_message_id`, `sender_deleted_at`, `sender_id`, `subject`, `updated_at`) values (1, 0, 'Hallo \' Herr Buissant\r\nich würde gerne wissen ob wir bei ihnen auch unser auto abstellen können,fahrräder leihen und wie es mit bettwäsche aussieht und was sie noch für fragen haben,\r\nliebe grüße christiane gottschalk', 5299810, '2013-12-20', '2013-12-27', '88DSCQRJ', '2013-11-23 18:20:10', 0, NULL, 3, NULL, NULL, NULL, NULL, 4754165, 0, NULL, NULL, 4995086, 'Buchungsanfrage', '2013-11-23 18:20:10')"""

    val parser = new InsertParser
    val insert = parser(stm).get

    assert(insert.tableName === "messages")
    assert(insert.fields("approved") === "1")
    assert(insert.fields("created_at") === "2013-11-23 18:20:10")
    assert(insert.fields("read_at") === "NULL")
  }

  test("ignores comments in the end of the statement") {
    val stm = "INSERT INTO `messages` (`approved`) VALUES (true) /* lol */"

    val r = (new InsertParser)(stm)

    assert(r.get.fields === Map("approved" -> "true"))
  }

  test("Returns a failure when string could not be parsed") {
    val failure = (new InsertParser)("invalid statement")

    println(failure)

    assert(failure.isFailure)
  }

  test("parses empty fields") {
    val stm = "INSERT INTO `messages` (`approved`) VALUES ('') "
    val r = (new InsertParser)(stm)

    assert(r.get.fields === Map("approved" -> ""))
  }
}
