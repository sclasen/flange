/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/21/11
 * Time: 4:12 PM
 */

package com.force.doozer.flange

import doozer.DoozerMsg
import akka.actor.Actor._
import akka.dispatch.Future
import org.apache.camel.impl.SimpleRegistry
import org.apache.camel.impl.DefaultCamelContext
import org.jboss.netty.channel.ChannelUpstreamHandler
import org.jboss.netty.channel.ChannelDownstreamHandler
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import collection.JavaConversions._
import collection.mutable.{HashMap, ArrayBuffer}
import akka.dispatch.CompletableFuture
import akka.config.Supervision._
import annotation.tailrec
import util.matching.Regex
import akka.camel.{Message, Failure, Producer, CamelServiceFactory}
import akka.event.EventHandler
import akka.actor.{MaximumNumberOfRestartsWithinTimeRangeReached, Actor}
import org.apache.camel.CamelExchangeException

object DoozerClient {
  implicit def stringToByteArray(value: String): Array[Byte] = value.getBytes("UTF-8")

  implicit def byteArrayToString(value: Array[Byte]): String = new String(value, "UTF-8")
}

trait DoozerClient {

  def get_!(path: String): GetResponse

  def get(path: String): Either[ErrorResponse, GetResponse]

  def getAsync(path: String)(callback: (Either[ErrorResponse, GetResponse] => Unit)): Unit

  def set_!(path: String, value: Array[Byte], rev: Long): SetResponse

  def set(path: String, value: Array[Byte], rev: Long): Either[ErrorResponse, SetResponse]

  def setAsync(path: String, value: Array[Byte], rev: Long)(callback: (Either[ErrorResponse, SetResponse] => Unit)): Unit

  def delete_!(path: String, rev: Long): DeleteResponse

  def delete(path: String, rev: Long): Either[ErrorResponse, DeleteResponse]

  def deleteAsync(path: String, rev: Long)(callback: (Either[ErrorResponse, DeleteResponse] => Unit)): Unit

  def rev_! : RevResponse

  def rev: Either[ErrorResponse, RevResponse]

  def revAsync(callback: (Either[ErrorResponse, RevResponse] => Unit))

  def wait_!(glob: String, rev: Long): WaitResponse

  def wait(glob: String, rev: Long): Either[ErrorResponse, WaitResponse]

  def waitAsync(glob: String, rev: Long)(callback: (Either[ErrorResponse, WaitResponse]) => Unit)

}

object Flange {
  lazy val simpleRegistry: SimpleRegistry = {
    val reg = new SimpleRegistry
    val decoders = new ArrayBuffer[ChannelUpstreamHandler]();
    decoders += (new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
    decoders += (new ProtobufDecoder(DoozerMsg.Response.getDefaultInstance));
    val encoders = new ArrayBuffer[ChannelDownstreamHandler]();
    encoders += (new LengthFieldPrepender(4));
    encoders += (new ProtobufEncoder());
    reg.put("encoders", bufferAsJavaList(encoders))
    reg.put("decoders", bufferAsJavaList(decoders))
    reg
  }
  lazy val camelService = CamelServiceFactory.createCamelService({
    val ctx = new DefaultCamelContext(simpleRegistry)
    ctx
  }).start

  def parseDoozerUri(doozerUri: String): List[String] = {
    """^doozer:\?(.*)$""".r.findFirstMatchIn(doozerUri) match {
      case Some(m@Regex.Match(_)) => {
        val doozerds = for {
          caServer <- m.group(1).split("&").toList
          k <- caServer.split("=").headOption if k == "ca"
          v <- caServer.split("=").tail.headOption
        } yield v
        doozerds
      }
      case _ => throw new IllegalArgumentException("cant parse doozerUri:" + doozerUri)
    }
  }


  val allConnectionsFailed = "ALL_CONNECTIONS_FAILED"
}

class Flange(doozerUri: String) extends DoozerClient {

  import Flange._

  private val doozerds = parseDoozerUri(doozerUri)
  private val service = camelService
  private val supervisor = actorOf(new ConnectionSupervisor(doozerds.size)).start()
  private val connection = {

    val state = new ClientState(doozerds.toIterable)
    val conn = actorOf(new ConnectionActor(state))
    supervisor.startLink(conn)
    conn
  }

  def stop() {
    connection.stop()
    supervisor.stop()
  }

  private def timeout = Left(ErrorResponse("CLIENT_TIMEOUT", "The operation timed out"))

  private def noConnections = Left(ErrorResponse(allConnectionsFailed, "Attempts to retry the operation at all configured servers failed"))

  private def exception(t: Throwable) = Left(ErrorResponse("DoozerClient Exception", t.getStackTraceString))

  private def retry[T](req: DoozerRequest)(success: PartialFunction[Any, Either[ErrorResponse, T]]): Either[ConnectionFailed, Either[ErrorResponse, T]] = {
    try {
      val resp = connection !! req
      if (success.isDefinedAt(resp)) Right(success(resp))
      else resp match {
        case Some(e@ErrorResponse(_, _)) => Right(Left(e))
        case Some(NoConnectionsLeft) => Right(noConnections)
        case None => Left(ConnectionFailed())
      }
    } catch {
      case e => Left(ConnectionFailed())
    }
  }

  @tailrec
  private def complete[T](req: DoozerRequest)(success: PartialFunction[Any, Either[ErrorResponse, T]]): Either[ErrorResponse, T] = {
    val res = retry[T](req)(success)
    res match {
      case Right(ok) => ok
      case Left(fail) => complete[T](req)(success)
    }
  }

  private def completeFuture[T](req: DoozerRequest, responseCallback: (Either[ErrorResponse, T] => Unit))(success: PartialFunction[Any, Either[ErrorResponse, T]]) {
    val future: Future[_] = connection !!! req
    future.asInstanceOf[Future[T]].onComplete {
      f: Future[T] =>
        if (success.isDefinedAt(f.value)) responseCallback(success(f.value))
        else {
          f.value match {
            case Some(Right(e@ErrorResponse(_, _))) => responseCallback(Left(e))
            case Some(Right(NoConnectionsLeft)) => responseCallback(noConnections)
            case _ => completeFuture(req, responseCallback)(success)
          }
        }
    }
  }


  def deleteAsync(path: String, rev: Long)(callback: (Either[ErrorResponse, DeleteResponse]) => Unit) {
    completeFuture[DeleteResponse](DeleteRequest(path, rev), callback) {
      case Some(Right(d@DeleteResponse(_))) => Right(d)
    }
  }


  def delete_!(path: String, rev: Long) = delete(path, rev) match {
    case Right(d@DeleteResponse(_)) => d
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def delete(path: String, rev: Long) = complete[DeleteResponse](DeleteRequest(path, rev)) {
    case Some(d@DeleteResponse(_)) => Right(d)
  }

  def setAsync(path: String, value: Array[Byte], rev: Long)(callback: (Either[ErrorResponse, SetResponse]) => Unit) {
    completeFuture[SetResponse](SetRequest(path, value, rev), callback) {
      case Some(Right(s@SetResponse(_))) => Right(s)
    }
  }

  def set_!(path: String, value: Array[Byte], rev: Long) = set(path, value, rev) match {
    case Right(s@SetResponse(_)) => s
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def set(path: String, value: Array[Byte], rev: Long) = complete[SetResponse](SetRequest(path, value, rev)) {
    case Some(s@SetResponse(_)) => Right(s)
  }

  def getAsync(path: String)(callback: (Either[ErrorResponse, GetResponse]) => Unit) {
    completeFuture[GetResponse](GetRequest(path), callback) {
      case Some(Right(g@GetResponse(_, _))) => Right(g)
    }
  }

  def get(path: String): Either[ErrorResponse, GetResponse] = complete[GetResponse](GetRequest(path)) {
    case Some(g@GetResponse(_, _)) => Right(g)
  }

  def get_!(path: String) = get(path) match {
    case Right(g@GetResponse(_, _)) => g
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def revAsync(callback: (Either[ErrorResponse, RevResponse]) => Unit) {
    completeFuture[RevResponse](RevRequest, callback) {
      case Some(Right(r@RevResponse(_))) => Right(r)
    }
  }

  def rev = complete[RevResponse](RevRequest) {
    case Some(r@RevResponse(_)) => Right(r)
  }

  def rev_! = rev match {
    case Right(r@RevResponse(_)) => r
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def waitAsync(glob: String, rev: Long)(callback: (Either[ErrorResponse, WaitResponse]) => Unit) = {
    completeFuture[WaitResponse](WaitRequest(glob, rev), callback) {
      case Some(Right(w@WaitResponse(_, _, _))) => Right(w)
    }
  }

  def wait(glob: String, rev: Long) = complete[WaitResponse](WaitRequest(glob, rev)) {
    case Some(w@WaitResponse(_, _, _)) => Right(w)
  }

  def wait_!(glob: String, rev: Long) = wait(glob, rev) match {
    case Right(w@WaitResponse(_, _, _)) => w
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }
}

class ConnectionSupervisor(numHosts: Int) extends Actor {
  self.faultHandler = OneForOneStrategy(List(classOf[Exception]), numHosts, numHosts * 1000)

  protected def receive = {
    case MaximumNumberOfRestartsWithinTimeRangeReached(_, _, _, ex) => EventHandler.error(ex, this, "Too Many Restarts")
  }
}

class ClientState(var hosts: Iterable[String], var tag: Int = 0)

class ConnectionFailedException(val host: String, cause: Throwable) extends RuntimeException(cause)

class ErrorResponseException(val resp: ErrorResponse) extends RuntimeException()

class ConnectionActor(state: ClientState) extends Actor with Producer {
  self.lifeCycle = Permanent

  private var host: String = null;
  private var requests = new HashMap[Int, DoozerRequest]
  private var responses = new HashMap[Int, Option[CompletableFuture[_]]]
  val tagHeader = "doozer.tag"

  lazy val endpointUri = {
    state.hosts.headOption match {
      case Some(h) => {
        host = h
        state.hosts = state.hosts.tail
        "netty:tcp://%s?encoders=#encoders&decoders=#decoders&disconnect=true".format(host)
      }
      case None => {
        become(noConn(), false)
        "direct://noConnections"
      }
    }
  }


  override def oneway = true

  override def preRestartProducer(reason: Throwable) {
    EventHandler.warning(this, "failed:" + endpointUri)
  }


  override def postRestart(reason: Throwable) {
    EventHandler.warning(this, "failTo:" + endpointUri)
  }

  private def noConn(): Receive = {
    case _ => self.reply(NoConnectionsLeft)
  }

  private def doSend(req: DoozerRequest): Message = {
    val currentTag = state.tag
    state.tag += 1
    requests += currentTag -> req
    responses += currentTag -> self.senderFuture
    Message(req.toBuilder.setTag(currentTag).build, Map(tagHeader -> currentTag))
  }

  override protected def receiveBeforeProduce = {
    case req: DoozerRequest => doSend(req)
  }


  override protected def receiveAfterProduce = {
    case DoozerResponse(response) => {
      requests.remove(response.getTag) match {
        case Some(req) => {
          val msg = DoozerResponse.isOk(response) match {
            case true => req.toResponse(response)
            case false => req.toError(response)
          }
          responses.get(response.getTag) match {
            case Some(future) => future.get.asInstanceOf[CompletableFuture[Any]].completeWithResult(msg)
            case None => EventHandler.warning(this, "Received a response with tag %d but there was no futute to complete".format(response.getTag))
          }
        }
        case None => EventHandler.warning(this, "Revieved a response with tag %d but there was no request to correlate with".format(response.getTag))

      }
    }
    case Failure(why, h) if why.isInstanceOf[CamelExchangeException] => throw new ConnectionFailedException(host, why)
    case Failure(why, h) => EventHandler.error(why, this, host)
  }


}

