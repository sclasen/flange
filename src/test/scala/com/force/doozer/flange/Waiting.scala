/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/25/11
 * Time: 2:01 PM
 */
package com.force.doozer.flange

import java.util.concurrent.CountDownLatch

trait Waiting {


  var latch = new CountDownLatch(1)

  def reset(count: Int) {
    while (latch.getCount > 0) latch.countDown()
    latch = new CountDownLatch(count)
  }

  def waitForAsync() {
    latch.await()
  }

  def signalAsyncDone() {
    latch.countDown()
  }
}