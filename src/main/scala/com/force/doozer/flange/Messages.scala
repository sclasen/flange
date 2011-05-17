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
import doozer.DoozerMsg.Response.Err
import doozer.DoozerMsg.Response

object DoozerRequest {

}

object DoozerResponse {

  val valid = 1
  val done = 2
  val set = 4
  val del = 8

  def isOk(msg: DoozerMsg.Response): Boolean = msg.getErrCode == null  || (msg.getErrCode == Err.OTHER &&( msg.getErrDetail eq  ""))
}

sealed trait DoozerRequest {
  type Response

  def builder = DoozerMsg.Request.newBuilder

  def toBuilder: DoozerMsg.Request.Builder

  def toResponse(res: DoozerMsg.Response): Response

  def toError(res: DoozerMsg.Response): ErrorResponse = ErrorResponse(res.getErrCode.name, res.getErrDetail)

}

case class ErrorResponse(code: String, description: String)

case class AccessRequest(secret:String) extends DoozerRequest{
  type Response = AccessResponse
  lazy val toBuilder = builder.setVerb(Verb.ACCESS).setValue(ByteString.copyFromUtf8(secret))

  def toResponse(res: DoozerMsg.Response) :AccessResponse = AccessResponse()
}

case class AccessResponse()

case class GetRequest(path: String, rev: Long) extends DoozerRequest {
  type Response = GetResponse
  lazy val toBuilder = {
    val b = builder.setVerb(Verb.GET).setPath(path)
    if (rev != 0L) b.setRev(rev)
    b
  }

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

case class SetResponse(rev: Long)


case class DeleteRequest(path: String, rev: Long) extends DoozerRequest {
  type Response = DeleteResponse
  lazy val toBuilder = builder.setVerb(Verb.DEL).setPath(path).setRev(rev)

  def toResponse(res: DoozerMsg.Response): DeleteResponse = DeleteResponse(res.getPath)
}

case class WaitRequest(glob: String, rev: Long) extends DoozerRequest {
  type Response = WaitResponse
  lazy val toBuilder = builder.setVerb(Verb.WAIT).setPath(glob).setRev(rev)

  def toResponse(res: DoozerMsg.Response): WaitResponse = WaitResponse(res.getPath, res.getValue.toByteArray, res.getRev)
}

case class WaitResponse(path: String, value: Array[Byte], rev: Long)

case class StatRequest(path: String, rev: Long) extends DoozerRequest {
  type Response = StatResponse
  lazy val toBuilder = builder.setVerb(Verb.STAT).setPath(path).setRev(rev)

  def toResponse(res: DoozerMsg.Response): StatResponse = StatResponse(res.getPath, res.getLen, res.getRev)
}

case class StatResponse(path: String, length: Int, rev: Long)


case class GetdirRequest(dir: String, rev: Long, offset: Int = 0) extends DoozerRequest {
  type Response = GetdirResponse
  lazy val toBuilder = builder.setVerb(Verb.GETDIR).setPath(dir).setRev(rev).setOffset(offset)

  def toResponse(res: DoozerMsg.Response): GetdirResponse = GetdirResponse(res.getPath, res.getRev)

  def next() = copy(offset = this.offset + 1)
}

case class GetdirResponse(path: String, rev: Long)

case class WalkRequest(path: String, rev: Long, offset:Int=0) extends DoozerRequest {
  type Response = WalkResponse
  lazy val toBuilder = builder.setVerb(Verb.WALK).setPath(path).setRev(rev).setOffset(offset)

  def toResponse(res: DoozerMsg.Response): WalkResponse = WalkResponse(res.getPath, res.getValue.toByteArray, res.getRev)
}

case class WalkResponse(path: String, value:Array[Byte], rev: Long)

case class DeleteResponse(path: String)

case class ConnectionFailed()

case object NoConnectionsLeft