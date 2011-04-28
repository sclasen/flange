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
  var uri = "doozer:?ca=localhost:12345&ca=localhost:8046&ca=localhost:8047"

  "A Doozer DoozerClient" must {
    "must get values correctly" in {
      val path = System.currentTimeMillis.toString
      val value = path + "--value"
      client.set("/" + path, value, 0L)
      debug("set breakpoint here for failover")
      client.get_!("/" + path).value must be(value.getBytes)
      client.getAsync("/" + path)(asyncGet(value, _))
      waitForAsync
    }

    "must get rev correctly" in {
      (client.rev_!.rev) > 0 must be(true)
    }

    "must wait correctly" in {
      reset(2)

      val path1 = "/" + System.currentTimeMillis.toString
      Thread.sleep(10)
      val path2 = "/" + System.currentTimeMillis.toString

      client.waitAsync(path1, 0L) {
        wr => signalAsyncDone()
      }
      client.waitAsync(path2, 0L) {
        wr => signalAsyncDone()
      }

      client.set_!(path1, path1, 0L)
      client.set_!(path2, path2, 0L)

      waitForAsync(10000) must be(true)

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