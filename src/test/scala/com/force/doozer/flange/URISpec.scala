package com.force.doozer.flange

/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/25/11
 * Time: 9:51 AM
 */

import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}


class URISpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  "A Flange" must {
    "parse doozer uris correctly" in {
      val one = "doozer:?ca=localhost:8046"
      Flange.parseDoozerUri(one).headOption must be(Some("localhost:8046"))
      val three = "doozer:?ca=localhost:8046&ca=localhost:8047&ca=localhost:8048"
      Flange.parseDoozerUri(three) must be(List("localhost:8046","localhost:8047","localhost:8048"))

    }
  }
}