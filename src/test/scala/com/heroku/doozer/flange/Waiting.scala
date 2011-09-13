/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/25/11
 * Time: 2:01 PM
 */
package com.heroku.doozer.flange

import java.util.concurrent.{TimeUnit, CountDownLatch}

trait Waiting {


  @volatile var latch = new CountDownLatch(1)

  def reset(count: Int) {
    while (latch.getCount > 0) latch.countDown()
    latch = new CountDownLatch(count)
  }

  def waitForAsync() {
    latch.await()
  }

  def waitForAsync(timeout: Long): Boolean = {
    latch.await(timeout, TimeUnit.MILLISECONDS)
  }

  def signalAsyncDone() {
    latch.countDown()
  }
}