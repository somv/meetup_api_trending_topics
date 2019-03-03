import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.humblefreak.analysis.{GroupData, RealTimeProcessing}
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class RealTimeProcessingSuite extends FunSuite with StreamingSuiteBase {

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

  test("should return the same list as the input") {
    def applyFilterTest(dStream: DStream[GroupData]): DStream[GroupData] = RealTimeProcessing.applyFilter(dStream, None, None, None)

    val grp1 = GroupData("hiking", 1, "us", "TX", "Austin")
    val grp2 = GroupData("adventure", 1, "us", "TX", "Austin")

    val inputList = List(List(grp1, grp2))
    val expectedList = List(List(grp1, grp2))

    testOperation(inputList, applyFilterTest _, expectedList, false)
  }

  test("should return the list containing group data from 'jp' country only") {
    def applyFilterTest(dStream: DStream[GroupData]): DStream[GroupData] = RealTimeProcessing.applyFilter(dStream, Some("jp"), None, None)

    val grp1 = GroupData("hiking", 1, "us", "TX", "Austin")
    val grp2 = GroupData("adventure", 1, "jp", "TK", "Tokyo")

    val inputList = List(List(grp1, grp2))
    val expectedList = List(List(grp2))

    testOperation(inputList, applyFilterTest _, expectedList, false)
  }

  test("should return the empty list after the filter") {
    def applyFilterTest(dStream: DStream[GroupData]): DStream[GroupData] = RealTimeProcessing.applyFilter(dStream, Some("jp"), Some("TKX"), None)

    val grp1 = GroupData("hiking", 1, "us", "TX", "Austin")
    val grp2 = GroupData("adventure", 1, "jp", "TK", "Tokyo")

    val inputList = List(List(grp1, grp2))
    val expectedList = List(List())

    testOperation(inputList, applyFilterTest _, expectedList, false)
  }

  test("make group data function testing") {
    def makeGroupDataTest(dStream: DStream[String]): DStream[GroupData] = RealTimeProcessing.makeGroupData(dStream)

    val responseString =
      """
        |{
        |  "venue": {
        |    "venue_name": "Zaffran (2nd floor)",
        |    "lon": 135.500488,
        |    "lat": 34.706043,
        |    "venue_id": 25505057
        |  },
        |  "visibility": "public",
        |  "response": "no",
        |  "guests": 0,
        |  "member": {
        |    "member_id": 188250948,
        |    "photo": "https://secure.meetupstatic.com/photos/member/6/9/7/d/thumb_273627005.jpeg",
        |    "member_name": "Yusuke"
        |  },
        |  "rsvp_id": 1774555264,
        |  "mtime": 1551507641029,
        |  "event": {
        |    "event_name": "☕️ English Cafe: All Levels, Casual Chat (Umeda)",
        |    "event_id": "258372388",
        |    "time": 1551510000000,
        |    "event_url": "https://www.meetup.com/Osaka-Social-Networking-and-Language-Exchange/events/258372388/"
        |  },
        |  "group": {
        |    "group_topics": [
        |      {
        |        "urlkey": "social",
        |        "topic_name": "Social"
        |      },
        |      {
        |        "urlkey": "newintown",
        |        "topic_name": "New In Town"
        |      },
        |      {
        |        "urlkey": "language",
        |        "topic_name": "Language & Culture"
        |      },
        |      {
        |        "urlkey": "culture-exchange",
        |        "topic_name": "Culture Exchange"
        |      },
        |      {
        |        "urlkey": "international-friends",
        |        "topic_name": "International Friends"
        |      },
        |      {
        |        "urlkey": "language-exchange",
        |        "topic_name": "Language Exchange"
        |      },
        |      {
        |        "urlkey": "exchangestud",
        |        "topic_name": "International and Exchange Students"
        |      },
        |      {
        |        "urlkey": "socialnetwork",
        |        "topic_name": "Social Networking"
        |      },
        |      {
        |        "urlkey": "dinner-parties",
        |        "topic_name": "Dinner Parties"
        |      },
        |      {
        |        "urlkey": "english-language",
        |        "topic_name": "English Language"
        |      },
        |      {
        |        "urlkey": "esl",
        |        "topic_name": "English as a Second Language"
        |      },
        |      {
        |        "urlkey": "young-professionals",
        |        "topic_name": "Young Professionals"
        |      },
        |      {
        |        "urlkey": "spanish",
        |        "topic_name": "Spanish Language"
        |      },
        |      {
        |        "urlkey": "french",
        |        "topic_name": "French Language"
        |      }
        |    ],
        |    "group_city": "Osaka",
        |    "group_country": "jp",
        |    "group_id": 18454353,
        |    "group_name": "Osaka Social Networking and Language Exchange",
        |    "group_lon": 135.5,
        |    "group_urlname": "Osaka-Social-Networking-and-Language-Exchange",
        |    "group_lat": 34.68
        |  }
        |}
      """.stripMargin

    val inputList = List(List(responseString))

    val expectedList = List(List(
      GroupData("social", 1, "jp", "", "osaka"),
      GroupData("new in town", 1, "jp", "", "osaka"),
      GroupData("language & culture", 1, "jp", "", "osaka"),
      GroupData("culture exchange", 1, "jp", "", "osaka"),
      GroupData("international friends", 1, "jp", "", "osaka"),
      GroupData("language exchange", 1, "jp", "", "osaka"),
      GroupData("international and exchange students", 1, "jp", "", "osaka"),
      GroupData("social networking", 1, "jp", "", "osaka"),
      GroupData("dinner parties", 1, "jp", "", "osaka"),
      GroupData("english language", 1, "jp", "", "osaka"),
      GroupData("english as a second language", 1, "jp", "", "osaka"),
      GroupData("young professionals", 1, "jp", "", "osaka"),
      GroupData("spanish language", 1, "jp", "", "osaka"),
      GroupData("french language", 1, "jp", "", "osaka")
    ))

    testOperation(inputList, makeGroupDataTest _, expectedList, false)
  }

}
