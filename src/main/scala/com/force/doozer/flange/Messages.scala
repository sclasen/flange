/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/22/11
 * Time: 2:57 PM
 */
package com.force.doozer.flange

import proto.DoozerMsg
import proto.DoozerMsg.Request.Verb
import com.google.protobuf.ByteString
import akka.camel.Message
import DoozerRequest._

object DoozerRequest {
  type WatchCallback = (WatchNotification => Unit)
}

object DoozerResponse {

  val valid = 1
  val done = 2
  val set = 4
  val del = 8

  def unapply(msg: Message): Option[DoozerMsg.Response] = {
    try {
      Some(msg.getBodyAs(classOf[DoozerMsg.Response]))
    } catch {
      case _ => None
    }
  }

  def isValid(msg: DoozerMsg.Response): Boolean = (msg.getFlags & valid) > 0

  def isDone(msg: DoozerMsg.Response): Boolean = (msg.getFlags & done) > 0

  def isSet(msg: DoozerMsg.Response): Boolean = (msg.getFlags & set) > 0

  def isDel(msg: DoozerMsg.Response): Boolean = (msg.getFlags & del) > 0

  def isOk(msg: DoozerMsg.Response): Boolean = msg.getErrCode == null
}

sealed trait DoozerRequest {
  type Response

  def builder = DoozerMsg.Request.newBuilder

  def toBuilder: DoozerMsg.Request.Builder

  def toResponse(res: DoozerMsg.Response): Response

  def toError(res: DoozerMsg.Response): ErrorResponse = ErrorResponse(res.getErrCode.name, res.getErrDetail)

}

case class ErrorResponse(code: String, description: String)

case class GetRequest(path: String) extends DoozerRequest {
  type Response = GetResponse
  lazy val toBuilder = builder.setVerb(Verb.GET).setPath(path)

  def toResponse(res: DoozerMsg.Response): GetResponse = GetResponse(res.getValue.toByteArray, res.getRev)
}

case class GetResponse(value: Array[Byte], cas: Long)

case class SetRequest(path: String, body: Array[Byte], cas: Long) extends DoozerRequest {
  type Response = SetResponse
  lazy val toBuilder = builder.setVerb(Verb.SET).setPath(path).setValue(ByteString.copyFrom(body)).setRev(cas)

  def toResponse(res: DoozerMsg.Response): SetResponse = SetResponse(res.getRev)
}

case class SetResponse(cas: Long)

case class WatchNotification(path: String, value: Array[Byte], cas: Long)

case class WatchRequest(path: String, cas: Long, callback:WatchCallback) extends DoozerRequest {
  type Response = WatchResponse
  lazy val toBuilder = builder.setVerb(Verb.WATCH).setPath(path).setRev(cas)

  def toResponse(res: DoozerMsg.Response): WatchResponse = WatchResponse(res.getPath)
}

case class WatchResponse(path: String)

case class DeleteRequest(path: String, cas: Long) extends DoozerRequest {
  type Response = DeleteResponse
  lazy val toBuilder = builder.setVerb(Verb.DEL).setPath(path).setRev(cas)

  def toResponse(res: DoozerMsg.Response): DeleteResponse = DeleteResponse(res.getPath)
}

case class DeleteResponse(path: String)

case class ConnectionFailed()

case object NoConnectionsLeft