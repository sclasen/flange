/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/22/11
 * Time: 2:57 PM
 */
package com.force.doozer.flange

import doozer.DoozerMsg
import doozer.DoozerMsg.Request.Verb
import com.google.protobuf.ByteString
import akka.camel.Message

object DoozerRequest {

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

case class GetResponse(value: Array[Byte], rev: Long)

case object RevRequest extends DoozerRequest {
  type Response = RevResponse
  lazy val toBuilder = builder.setVerb(Verb.REV)

  def toResponse(res: DoozerMsg.Response): RevResponse = RevResponse(res.getRev)
}

case class RevResponse(rev: Long)

case class SetRequest(path: String, body: Array[Byte], rev: Long) extends DoozerRequest {
  type Response = SetResponse
  lazy val toBuilder = builder.setVerb(Verb.SET).setPath(path).setValue(ByteString.copyFrom(body)).setRev(rev)

  def toResponse(res: DoozerMsg.Response): SetResponse = SetResponse(res.getRev)
}

case class SetResponse(cas: Long)


case class DeleteRequest(path: String, rev: Long) extends DoozerRequest {
  type Response = DeleteResponse
  lazy val toBuilder = builder.setVerb(Verb.DEL).setPath(path).setRev(rev)

  def toResponse(res: DoozerMsg.Response): DeleteResponse = DeleteResponse(res.getPath)
}

case class WaitRequest(glob: String, rev: Long) extends DoozerRequest {
  type Response = WaitResponse
  lazy val toBuilder = builder.setVerb(Verb.WAIT).setPath(glob)

  def toResponse(res: DoozerMsg.Response): WaitResponse = WaitResponse(res.getPath, res.getValue.toByteArray, res.getRev)
}

case class WaitResponse(path: String, value: Array[Byte], rev: Long)

case class DeleteResponse(path: String)

case class ConnectionFailed()

case object NoConnectionsLeft