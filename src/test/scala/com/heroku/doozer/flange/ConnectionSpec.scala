/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/21/11
 * Time: 9:36 PM
 */
package com.heroku.doozer.flange

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.actor.Actor._
import akka.actor.ActorRef
import DoozerRequest._
import Flange._
class ConnectionSpec extends WordSpec with MustMatchers {


  "a test" must {
    "work" in {
      var rev = 0L
      actorOf(new ConnectionActor(new ClientState("secret",List("localhost:8046").toIterable))).start()
      val ref: ActorRef = registry.actorsFor[ConnectionActor].head
      ref !! GetRequest("/d/local/foo",rev) match {
        case Some(GetResponse(_, curr)) => {
          rev = curr
          print("got")
        }
        case Some(e@ErrorResponse(_, _)) => {
          print(e)
        }
        case x@_ => print(x)
      }




      ref !! SetRequest("/d/local/foo", ("bar" + rev).getBytes, rev) match {
        case Some(SetResponse(cas)) => {
          print("Set")
          rev = cas
        }
        case Some(ErrorResponse(code, detail)) => {
          print(code)
          print(detail)
        }
        case x@_ => print(x)
      }
      ref !! SetRequest("/d/local/foo", ("bar" + rev).getBytes, rev) match {
        case Some(SetResponse(cas)) => {
          print("Set")
          rev = cas
        }
        case Some(ErrorResponse(code, detail)) => {
          print(code)
          print(detail)
        }
        case x@_ => print(x)
      }

      registry.shutdownAll()
    }
  }

  def print(any: Any) {
    System.out.println("==========>" + any)
  }

}