/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/23/11
 * Time: 12:14 PM
 */
package com.force.doozer.flange

import org.scalatest.WordSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.MustMatchers
import akka.actor.Actor._
import com.force.doozer.flange.DoozerClient._

class ClientSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with Waiting {

  var client: Flange = null
  var uri = "doozer:?ca=localhost:12321&ca=localhost:8046"

  "A Doozer Client" must {
    "set and get and delete values correctly" in {
      (1 to 20) foreach {
        i => {
          val path = "/" + System.currentTimeMillis.toString
          val value = path + "--value"
          val response: SetResponse = client.set_!(path, value, 0L)
          debug("set breakpoint here for failover")
          client.get_!(path).value must be(value.getBytes)
          client.delete_!(path, response.rev)
        }
      }
      val path = "/" + System.currentTimeMillis.toString
      val value = path + "--value"
      val response: SetResponse = client.set_!(path, value, 0L)
      client.getAsync(path)(asyncGet(value, _))
      waitForAsync()
      client.delete_!(path, response.rev)
    }



    "get rev correctly" in {
      (client.rev_!.rev) > 0 must be(true)
    }

    "wait correctly" in {
      reset(1)

      val path1 = "/" + System.currentTimeMillis.toString
      Thread.sleep(10)
      val path2 = "/" + System.currentTimeMillis.toString

      val response: SetResponse = client.set_!(path1, path1, 0L)
      val response2: SetResponse = client.set_!(path2, path2, 0L)

      client.waitAsync(path1, response.rev) {
        wr => {
          wr match {
            case Right(w@WaitResponse(_, value, _)) => {
              System.out.println(w.toString)
              System.out.println(new String(value))
              signalAsyncDone()
            }
            case Left(ErrorResponse(code, msg)) => System.out.println(code + " " + msg)
          }
        }
      }

      client.waitAsync(path2, response2.rev) {
        wr => {
          wr match {
            case Right(w@WaitResponse(_, value, _)) => {
              System.out.println(w.toString)
              System.out.println(new String(value))
              signalAsyncDone()
            }
            case Left(ErrorResponse(code, msg)) => System.out.println(code + " " + msg)
          }


        }
      }

      client.set_!(path2, path1, response2.rev)
      client.set_!(path1, path2, response.rev)

      waitForAsync(10000) must be(true)
      System.out.println("DONE")

    }


  }

  def asyncGet(value: String, resp: Either[ErrorResponse, GetResponse]) {
    resp match {
      case Right(GetResponse(respValue, cas)) => {
        respValue must be(value.getBytes)
        signalAsyncDone()
      }
      case x@_ => failure(x)
    }
  }


  def debug(any: Any) {
    any.toString
  }

  def failure(any: Any) {
    System.out.println("Error in Async GET")
    fail("Bad response")
  }

  "Two Clients" must {
    "not blow up" in {
      val second = new Flange(uri)
      second.set("/second", "second" getBytes, 0)
      client.get_!("/second").value must be("second".getBytes)
    }
  }

  override protected def beforeAll(configMap: Map[String, Any]) {
    client = new Flange(uri)
  }

  override protected def afterAll() {
    registry.shutdownAll()
  }
}