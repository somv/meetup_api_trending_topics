import com.humblefreak.analysis.{GroupData, RealTimeProcessing}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class RealTimeProcessingSuite extends FunSuite {

  test("makeGroupDataFromResString should run okay in case of valid response string") {

    val responseString =
      """
        |{
        |  "venue":{
        |     "venue_name":"Bahnhof Dauenhof",
        |     "lon":9.680496,
        |     "lat":53.852585,
        |     "venue_id":26199422
        |  },
        |  "visibility":"public",
        |  "response":"yes",
        |  "guests":0,
        |  "member":{
        |     "member_id":251786064,
        |     "photo":"https://secure.meetupstatic.com/photos/member/b/e/5/3/thumb_277788723.jpeg",
        |     "member_name":"Wander_liebe"
        |   },
        |   "rsvp_id":1774592428,
        |   "mtime":1551509469784,
        |   "event":{
        |       "event_name":"Stör and Breitenburg canal (21 km)",
        |       "event_id":"259243533",
        |       "time":1553419800000,
        |       "event_url":"https://www.meetup.com/Hamburg-Hikers/events/259243533/"
        |    },
        |    "group":{
        |       "group_topics":[
        |           {"urlkey":"hiking","topic_name":"Hiking"},
        |           {"urlkey":"outdoors","topic_name":"Outdoors"},
        |           {"urlkey":"excercise","topic_name":"Exercise"},
        |           {"urlkey":"adventure","topic_name":"Adventure"}
        |        ],
        |        "group_city":"Hamburg",
        |        "group_country":"de",
        |        "group_id":18605921,
        |        "group_name":"Hamburg Hikers",
        |        "group_lon":10,
        |        "group_urlname":"Hamburg-Hikers",
        |        "group_lat":53.55
        |    }
        |}
      """.stripMargin

    val expectedOutput: Seq[GroupData] = ArrayBuffer(
      GroupData("hiking", 1, "de", "", "hamburg"),
      GroupData("outdoors", 1, "de", "", "hamburg"),
      GroupData("exercise", 1, "de", "", "hamburg"),
      GroupData("adventure", 1, "de", "", "hamburg"))

    val actualOutput = RealTimeProcessing.makeGroupDataFromResString(responseString)

    assert(actualOutput.toString.equals(expectedOutput.toString))
  }


  test("makeGroupDataFromResString should return empty constant GroupData object in case of invalid response string") {

    val responseString =
      """
        |{
        |  "venue":{
        |     "venue_name":"Bahnhof Dauenhof",
        |     "lon":9.680496,
        |     "lat":53.852585,
        |     "venue_id":26199422
        |  },
        |  "visibility":"public",
        |  "response":"yes",
        |  "guests":0,
        |  "member":{
        |     "member_id":251786064,
        |     "photo":"https://secure.meetupstatic.com/photos/member/b/e/5/3/thumb_277788723.jpeg",
        |     "member_name":"Wander_liebe"
        |   },
        |   "rsvp_id":1774592428,
        |   "mtime":1551509469784,
        |   "event":{
        |       "event_name":"Stör and Breitenburg canal (21 km)",
        |       "event_id":"259243533",
        |       "time":1553419800000,
        |       "event_url":"https://www.meetup.com/Hamburg-Hikers/events/259243533/"
        |    }
      """.stripMargin

    val actualOutput = RealTimeProcessing.makeGroupDataFromResString(responseString)

    val expectedOutput = List(GroupData("", 0, "", "", ""))

    assert(actualOutput.toString.equals(expectedOutput.toString))
  }

}
