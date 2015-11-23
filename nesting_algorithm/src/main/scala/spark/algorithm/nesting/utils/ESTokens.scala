package spark.algorithm.nesting.utils

import scalaj.http._
import org.json4s._
import org.json4s.jackson.JsonMethods._

class ESTokens {

  def getResult():String = {

    val propFile = "/conf/monitors.properties"
    val prop = ConfigUtils.getConfig(propFile)
    val server = prop.get("server")
    val user = prop.get("user")
    val passwd = prop.get("passwd")
    val space = prop.get("space")
    val organization = prop.get("organization")

    val data = Map("user" -> user.get,
      "passwd" -> passwd.get,
      "space" -> space.get,
      "organization" -> organization.get
    ).toSeq

    val response = Http("https://"+server.get+"/login").timeout(100000,100000).postForm(data).asString

    val a = response.body
    val response4Json = parse(a)
    val tokens_tmp = "{'logging_token':" + compact(response4Json \ "logging_token") +
      ",'access_token':" + compact(response4Json \ "access_token") +
      ", 'space_id':" + compact(response4Json \ "space_id")+"}"

    val date_tags = "2015.11.06"
    val start_ts = 1446350066000l

    val x_auth_token = compact(response4Json \ "access_token").replaceAll("\"","")
    val x_auth_project_id = compact(response4Json \ "space_id").replaceAll("\"","")
    val request_headers = Seq(("X-Auth-Project-Id",x_auth_project_id),("X-Auth-Token",x_auth_token))

    val log_name = "logstash-" + compact(response4Json \ "space_id") + "-" + date_tags
    val resource_path = "/elasticsearch/" + log_name + "/_search"
    val search_url = "https://" + server.get + resource_path

    val request_data =s"""{
                         |      "query": {
                         |        "filtered": {
                         |          "query": {
                         |            "bool": {
                         |              "should": [
                         |                {
                         |                  "query_string": {
                         |                    "query": "log:packetbeat AND log:direction"
                         |                  }
                         |                }
                         |              ]
                         |            }
                         |          },
                         |          "filter": {
                         |            "bool": {
                         |              "must": [
                         |                {
                         |                  "range": {
                         |                    "@timestamp": {
                         |                      "gte": $start_ts
                         |
                         |                    }
                         |                  }
                         |                }
                         |              ]
                         |            }
                         |          }
                         |        }
                         |      },
                         |      "size": 500,
                         |      "sort": [
                         |        {
                         |          "@timestamp": {
                         |            "order": "desc",
                         |            "ignore_unmapped": true
                         |          }
                         |        },
                         |        {
                         |          "@timestamp": {
                         |            "order": "desc",
                         |            "ignore_unmapped": true
                         |          }
                         |        }
                         |      ]
                         |    }""".stripMargin

    val query_response = Http(search_url.replaceAll("\"","")).timeout(100000,100000).headers(request_headers).postData(request_data).asString

    return query_response.body
  }

}
