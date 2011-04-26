package com.force.doozer.flange

/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/25/11
 * Time: 12:21 PM
 */

import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import akka.actor.Actor

class FailoverSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with Waiting {

  var client: Flange = null


  "A Doozer DoozerClient" must {
    "fail over properly when no servers are up" in {
      val clientWithNoServersUp = new Flange("doozer:?ca=localhost:12321&ca=localhost:12322")
      try {
        clientWithNoServersUp.set("/nothome", "avalue".getBytes, 0L) match {
          case Left(ErrorResponse(err, _)) => err must be(Flange.allConnectionsFailed)
          case Right(SetResponse(_)) => fail("Set shouldnt success with no server")
        }
        clientWithNoServersUp.getAsync("/nothome") {
          either =>
            try {
              either match {
                case Left(ErrorResponse(err, _)) => {
                  err must be(Flange.allConnectionsFailed)
                }
                case Right(GetResponse(_, _)) => fail("get shouldnt success with no server")
              }
            } finally {
              signalAsyncDone()
            }
        }
        waitForAsync()
      } catch {
        case e => {
          signalAsyncDone()
          fail(e)
        }
      } finally {
        clientWithNoServersUp.stop
        stop()
      }
    }


  }

  def stop() {
    Actor.registry.shutdownAll()
  }

}