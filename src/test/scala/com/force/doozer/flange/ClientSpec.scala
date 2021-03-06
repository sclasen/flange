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
  var uri = "doozer:?ca=localhost:12345&ca=localhost:8046&sk=secret"

  "A Doozer Client" must {
    "set and get and stat and delete values correctly" in {
      System.out.println("Exceptions are to be expected here as we purposely use a url with a host thats not up to test failover")
      (1 to 20) foreach {
        i => {
          val path = "/" + System.currentTimeMillis.toString
          val value = path + "--value"
          val response: SetResponse = client.set_!(path, value, 0L)
          val getResponse: GetResponse = client.get_!(path)
          getResponse.value must be(value.getBytes)
          client.stat_!(path,getResponse.rev).length must be(getResponse.value.length)
          client.delete_!(path, response.rev)
        }
      }
      val path = "/" + System.currentTimeMillis.toString
      val value = path + "--value"
      val value2 = path + "--value2"
      val response: SetResponse = client.set_!(path, value, 0L)
      val response2: SetResponse = client.set_!(path, value2, response.rev)
      client.getAsync(path)(asyncGet(value2, _))
      waitForAsync(1000) must be (true)
      client.get_!(path, response.rev).value must be(value.getBytes)
      client.get_!(path,response2.rev).value must be(value2.getBytes)
      client.delete_!(path, response2.rev)
    }


    "getdir correctly" in{
      val path = "/" + System.currentTimeMillis.toString
      val a = path + "/a"
      val b = path + "/b"
      val c = path + "/c"
      val resA: SetResponse = client.set_!(a, a, 0)
      val resB: SetResponse = client.set_!(b, b, 0)
      val resC: SetResponse = client.set_!(c, c, 0)

      client.getdir_!(path, resA.rev, 0).path must be("a")
      evaluating {client.getdir_!(path,resA.rev,1)} must produce[ErrorResponseException]

      client.getdir_!(path, resB.rev, 0).path must be("a")
      client.getdir_!(path, resB.rev, 1).path must be("b")
      evaluating {client.getdir_!(path,resB.rev,2)} must produce[ErrorResponseException]

      client.getdir_!(path, resC.rev, 0).path must be("a")
      client.getdir_!(path, resC.rev, 1).path must be("b")
      client.getdir_!(path, resC.rev, 2).path must be("c")
      evaluating {client.getdir_!(path,resC.rev,3)} must produce[ErrorResponseException]

      client.getdir_all_!(path, resC.rev).map(_.path) must be(List("a","b","c"))
    }

    "walk correctly" in{
      val path = "/" + System.currentTimeMillis.toString
      val pathglob = path + "/*"
      val a = path + "/a"
      val b = path + "/b"
      val c = path + "/c"
      val resA: SetResponse = client.set_!(a, a, 0)
      val resB: SetResponse = client.set_!(b, b, 0)
      val resC: SetResponse = client.set_!(c, c, 0)

      client.walk_!(pathglob, resA.rev, 0).path must be(a)
      client.walk_!(pathglob, resA.rev, 0).value must be(a.getBytes)
      evaluating {client.walk_!(path,resA.rev,1)} must produce[ErrorResponseException]

      client.walk_!(pathglob, resB.rev, 0).path must be(a)
      client.walk_!(pathglob, resB.rev, 1).path must be(b)
      client.walk_!(pathglob, resB.rev, 0).value must be(a.getBytes)
      client.walk_!(pathglob, resB.rev, 1).value must be(b.getBytes)
      evaluating {client.walk_!(path,resB.rev,2)} must produce[ErrorResponseException]

      client.walk_!(pathglob, resC.rev, 0).path must be(a)
      client.walk_!(pathglob, resC.rev, 1).path must be(b)
      client.walk_!(pathglob, resC.rev, 2).path must be(c)
      client.walk_!(pathglob, resC.rev, 0).value must be(a.getBytes)
      client.walk_!(pathglob, resC.rev, 1).value must be(b.getBytes)
      client.walk_!(pathglob, resC.rev, 2).value must be(c.getBytes)
      evaluating {client.walk_!(path,resC.rev,3)} must produce[ErrorResponseException]

      client.walk_all_!(pathglob,resC.rev).map(w=>new String(w.value)) must be(List(a,b,c))
      client.walk_all_!(pathglob,resC.rev).map(_.path) must be(List(a,b,c))
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

      var set: SetResponse = client.set_!(path2, path1, response2.rev)
      client.set_!(path1, path2, response.rev)

      waitForAsync(10000) must be(true)
      System.out.println("DONE")

      reset(2)
      client.watch(path2,set.rev){
        either=>{
          debug(either)
          signalAsyncDone()
          true
        }
      }

      set = client.set_!(path2,"foowait1",set.rev)
      client.set_!(path2,"foowait2",set.rev)
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
    System.out.println(any.toString)
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