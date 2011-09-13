/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/21/11
 * Time: 4:12 PM
 */

package com.heroku.doozer.flange

import doozer.DoozerMsg
import doozer.DoozerMsg.Response.Err
import akka.actor.Actor._
import akka.dispatch.Future
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import collection.mutable.HashMap
import akka.dispatch.CompletableFuture
import akka.config.Supervision._
import annotation.tailrec
import util.matching.Regex

import akka.event.EventHandler
import akka.actor.{MaximumNumberOfRestartsWithinTimeRangeReached, Actor}
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.lang.{RuntimeException, Thread}


object DoozerClient {
  implicit def stringToByteArray(value: String): Array[Byte] = value.getBytes("UTF-8")

  implicit def byteArrayToString(value: Array[Byte]): String = new String(value, "UTF-8")
}

trait DoozerClient {

  def get_!(path: String, rev: Long = 0L): GetResponse

  def get(path: String, rev: Long = 0L): Either[ErrorResponse, GetResponse]

  def getAsync(path: String, rev: Long = 0L)(callback: (Either[ErrorResponse, GetResponse] => Unit)): Unit

  def set_!(path: String, value: Array[Byte], rev: Long): SetResponse

  def set(path: String, value: Array[Byte], rev: Long): Either[ErrorResponse, SetResponse]

  def setAsync(path: String, value: Array[Byte], rev: Long)(callback: (Either[ErrorResponse, SetResponse] => Unit)): Unit

  def delete_!(path: String, rev: Long): DeleteResponse

  def delete(path: String, rev: Long): Either[ErrorResponse, DeleteResponse]

  def deleteAsync(path: String, rev: Long)(callback: (Either[ErrorResponse, DeleteResponse] => Unit)): Unit

  def rev_! : RevResponse

  def rev: Either[ErrorResponse, RevResponse]

  def revAsync(callback: (Either[ErrorResponse, RevResponse] => Unit))

  def wait_!(glob: String, rev: Long, waitFor: Long = Long.MaxValue): WaitResponse

  def wait(glob: String, rev: Long, waitFor: Long = Long.MaxValue): Either[ErrorResponse, WaitResponse]

  def waitAsync(glob: String, rev: Long, waitFor: Long = Long.MaxValue)(callback: (Either[ErrorResponse, WaitResponse]) => Unit)

  def stat_!(path: String, rev: Long): StatResponse

  def stat(path: String, rev: Long): Either[ErrorResponse, StatResponse]

  def statAsync(path: String, rev: Long)(callback: (Either[ErrorResponse, StatResponse]) => Unit)

  def getdir(dir: String, rev: Long, offset: Int): Either[ErrorResponse, GetdirResponse]

  def getdir_!(dir: String, rev: Long, offset: Int): GetdirResponse

  def getdirAsync(dir: String, rev: Long, offset: Int)(callback: (Either[ErrorResponse, GetdirResponse]) => Unit)

  def walk(glob: String, rev: Long, offset: Int): Either[ErrorResponse, WalkResponse]

  def walk_!(glob: String, rev: Long, offset: Int): WalkResponse

  def walkAsync(glob: String, rev: Long, offset: Int)(callback: (Either[ErrorResponse, WalkResponse]) => Unit)

  def walk_all(glob: String, rev: Long): Either[ErrorResponse, List[WalkResponse]]

  def getdir_all(dir: String, rev: Long): Either[ErrorResponse, List[GetdirResponse]]

  def walk_all_!(glob: String, rev: Long): List[WalkResponse]

  def getdir_all_!(dir: String, rev: Long): List[GetdirResponse]

  def watch(glob: String, rev: Long, watchFor: Long = Long.MaxValue)(callback: (Either[ErrorResponse, WaitResponse]) => Boolean)


}

object Flange {

  lazy val daemonThreadFactory = new ThreadFactory {
    val count = new AtomicInteger(0)

    def newThread(r: Runnable) = {
      val t = new Thread(r, "FlangeConnector:" + count.incrementAndGet())
      t.setDaemon(true)
      t
    }
  }

  def parseDoozerUri(doozerUri: String): (List[String], String) = {
    """^doozer:\?(.*)$""".r.findFirstMatchIn(doozerUri) match {
      case Some(m@Regex.Match(_)) => {
        val doozerds = for {
          caServer <- m.group(1).split("&").toList
          k <- caServer.split("=").headOption if k == "ca"
          v <- caServer.split("=").tail.headOption
        } yield v
        val sk = for {
          sks <- m.group(1).split("&").toList
          k <- sks.split("=").headOption if k == "sk"
          v <- sks.split("=").tail.headOption
        } yield v

        (doozerds, sk.headOption.getOrElse(throw new IllegalArgumentException("Missing sk param")))
      }
      case _ => throw new IllegalArgumentException("cant parse doozerUri:" + doozerUri)
    }
  }

  def eachDoozerOnceStrategy(doozerds: List[String]): Iterable[String] = {
    doozerds.toIterable
  }

  def retryForeverStrategy(doozerds: List[String]): Iterable[String] = {
    var cur = doozerds
    Stream.continually {
      cur.headOption match {
        case Some(doozer) => {
          cur = cur.tail
          doozer
        }
        case None => {
          cur = doozerds.tail
          doozerds.head
        }
      }
    }
  }

  val allConnectionsFailed = "ALL_CONNECTIONS_FAILED"
}


import Flange._


class Flange(doozerUri: String, failoverStrategy: List[String] => Iterable[String] = eachDoozerOnceStrategy) extends DoozerClient {

  private val (doozerds, sk) = parseDoozerUri(doozerUri)
  private val supervisor = actorOf(new ConnectionSupervisor(doozerds.size)).start()
  private val connection = {
    val state = new ClientState(sk, failoverStrategy(doozerds))
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
      val resp = connection !! (req, req.timeout)
      if (resp.isDefined && success.isDefinedAt(resp.get)) Right(success(resp.get))
      else resp match {
        case Some(e@ErrorResponse(_, desc)) if desc equals "permission denied" => {
          connection !! AccessRequest(sk) match {
            case Some(r: AccessResponse) => retry(req)(success)
            case er@_ => {
              EventHandler.error(er, "cant auth")
              Left(ConnectionFailed())
            }
          }
        }
        case Some(e@ErrorResponse(_, _)) => Right(Left(e))
        case Some(NoConnectionsLeft) => Right(noConnections)
        case None => Left(ConnectionFailed())
      }
    } catch {
      case e =>
        EventHandler.error(e, this, "error")
        Left(ConnectionFailed())
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
    val future: Future[_] = connection !!! (req, req.timeout)
    future.asInstanceOf[Future[T]].onComplete {
      f: Future[T] =>
        if (success.isDefinedAt(f.value)) responseCallback(success(f.value))
        else {
          f.value match {
            case Some(Right(e@ErrorResponse(_, desc))) if desc equals "permission denied" => {
              connection !! AccessRequest(sk) match {
                case Some(r: AccessResponse) => retry(req)(success)
                case er@_ => {
                  EventHandler.error(er, "cant auth")
                  responseCallback(Left(e))
                }
              }
            }
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
    case d@DeleteResponse(_) => Right(d)
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
    case s@SetResponse(_) => Right(s)
  }

  def getAsync(path: String, rev: Long = 0L)(callback: (Either[ErrorResponse, GetResponse]) => Unit) {
    completeFuture[GetResponse](GetRequest(path, rev), callback) {
      case Some(Right(g@GetResponse(_, _))) => Right(g)
    }
  }

  def get(path: String, rev: Long = 0L): Either[ErrorResponse, GetResponse] = complete[GetResponse](GetRequest(path, rev)) {
    case g@GetResponse(_, _) => Right(g)
  }

  def get_!(path: String, rev: Long = 0L) = get(path, rev) match {
    case Right(g@GetResponse(_, _)) => g
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def revAsync(callback: (Either[ErrorResponse, RevResponse]) => Unit) {
    completeFuture[RevResponse](RevRequest, callback) {
      case Some(Right(r@RevResponse(_))) => Right(r)
    }
  }

  def rev = complete[RevResponse](RevRequest) {
    case r@RevResponse(_) => Right(r)
  }

  def rev_! = rev match {
    case Right(r@RevResponse(_)) => r
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def waitAsync(glob: String, rev: Long, waitFor: Long = Long.MaxValue)(callback: (Either[ErrorResponse, WaitResponse]) => Unit) = {
    completeFuture[WaitResponse](WaitRequest(glob, rev, waitFor), callback) {
      case Some(Right(w@WaitResponse(_, _, _))) => Right(w)
    }
  }

  def wait(glob: String, rev: Long, waitFor: Long = Long.MaxValue) = complete[WaitResponse](WaitRequest(glob, rev, waitFor)) {
    case w@WaitResponse(_, _, _) => Right(w)
  }

  def wait_!(glob: String, rev: Long, waitFor: Long = Long.MaxValue) = wait(glob, rev, waitFor) match {
    case Right(w@WaitResponse(_, _, _)) => w
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def statAsync(path: String, rev: Long)(callback: (Either[ErrorResponse, StatResponse]) => Unit) = {
    completeFuture[StatResponse](StatRequest(path, rev), callback) {
      case Some(Right(s@StatResponse(_, _, _))) => Right(s)
    }
  }

  def stat(path: String, rev: Long) = complete[StatResponse](StatRequest(path, rev)) {
    case s@StatResponse(_, _, _) => Right(s)
  }


  def stat_!(path: String, rev: Long) = stat(path, rev) match {
    case Right(s@StatResponse(_, _, _)) => s
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def getdir(dir: String, rev: Long, offset: Int) = complete[GetdirResponse](GetdirRequest(dir, rev, offset)) {
    case g@GetdirResponse(_, _) => Right(g)
  }

  def getdir_!(dir: String, rev: Long, offset: Int) = getdir(dir, rev, offset) match {
    case Right(g@GetdirResponse(_, _)) => g
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def getdirAsync(dir: String, rev: Long, offset: Int)(callback: (Either[ErrorResponse, GetdirResponse]) => Unit) = {
    completeFuture[GetdirResponse](GetdirRequest(dir, rev, offset), callback) {
      case Some(Right(g@GetdirResponse(_, _))) => Right(g)
    }
  }

  def walk(glob: String, rev: Long, offset: Int) = complete[WalkResponse](WalkRequest(glob, rev, offset)) {
    case w@WalkResponse(_, _, _) => Right(w)
  }

  def walk_!(glob: String, rev: Long, offset: Int) = walk(glob, rev, offset) match {
    case Right(w@WalkResponse(_, _, _)) => w
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def walkAsync(glob: String, rev: Long, offset: Int)(callback: (Either[ErrorResponse, WalkResponse]) => Unit) = {
    completeFuture[WalkResponse](WalkRequest(glob, rev, offset), callback) {
      case Some(Right(w@WalkResponse(_, _, _))) => Right(w)
    }
  }

  def watch(glob: String, rev: Long, waitFor: Long = Long.MaxValue)(callback: (Either[ErrorResponse, WaitResponse]) => Boolean) = {
    def inner(r: Long, either: Either[ErrorResponse, WaitResponse]) {
      if (callback.apply(either)) {
        either match {
          case Left(_) =>  waitAsync(glob, r , waitFor)(inner(r , _))
          case Right(WaitResponse(_,_,newRev)) => waitAsync(glob, newRev+1 , waitFor)(inner(newRev+1 , _))
        }
      }
    }
    waitAsync(glob, rev, waitFor)(inner(rev, _))
  }

  def getdir_all(dir: String, rev: Long) = {
    all_internal[GetdirResponse](getdir(dir, rev, _), 0, Nil)
  }

  @tailrec
  private def all_internal[T](func: Int => Either[ErrorResponse, T], offset: Int, responses: List[T]): Either[ErrorResponse, List[T]] = {
    func.apply(offset) match {
      case Left(ErrorResponse(code, msg)) if code eq Err.RANGE.name() => Right(responses)
      case Left(e@ErrorResponse(_, _)) => Left(e)
      case Right(t: T) => all_internal(func, offset + 1, responses :+ t)
    }
  }

  def walk_all(glob: String, rev: Long) = {
    all_internal[WalkResponse](walk(glob, rev, _), 0, Nil)
  }

  def getdir_all_!(dir: String, rev: Long) = getdir_all(dir, rev) match {
    case Right(responses) => responses
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }

  def walk_all_!(glob: String, rev: Long) = walk_all(glob, rev) match {
    case Right(responses) => responses
    case Left(e@ErrorResponse(_, _)) => throw new ErrorResponseException(e)
  }
}

class ConnectionSupervisor(numHosts: Int) extends Actor {
  self.faultHandler = OneForOneStrategy(List(classOf[Exception]), numHosts, numHosts * 1000)

  protected def receive = {
    case MaximumNumberOfRestartsWithinTimeRangeReached(_, _, _, ex) => EventHandler.error(ex, this, "Too Many Restarts")
  }
}

class ClientState(val secret: String, var hosts: Iterable[String], var tag: Int = 0)

class ConnectionFailedException(val host: String, cause: Throwable) extends RuntimeException(cause)

class ErrorResponseException(val resp: ErrorResponse) extends RuntimeException()

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.Channel
import java.util.concurrent.Executors
import java.net.InetSocketAddress
import com.heroku.doozer.flange.Flange._


class ConnectionActor(state: ClientState) extends Actor {
  self.lifeCycle = Permanent

  private var host: String = null
  private var port: Int = 0
  private var requests = new HashMap[Int, DoozerRequest]
  private var responses = new HashMap[Int, Option[CompletableFuture[_]]]
  private var connected = false
  private var bootstrap: ClientBootstrap = null
  private var handler: Handler = null
  private var channel: Channel = null


  state.hosts.headOption match {
    case Some(h) => {
      host = h.split(":").apply(0)
      port = h.split(":").apply(1).toInt
      state.hosts = state.hosts.tail
    }
    case None => {
      become(noConn(), false)
    }
  }

  private def notifyWaiters(ex: Throwable) {
    for {
      futureOpt <- responses.values
      future <- futureOpt
    } future.completeWithException(ex)
  }

  override def postStop() {
    notifyWaiters(new RuntimeException("Connection actor was stopped"))
  }

  override def preRestart(reason: Throwable) {
    EventHandler.warning(this, "failed:" + host + ":" + port)
    try {
      if (channel != null) channel.close()
    }
    catch {
      case _ =>
    }
    try {
      if (bootstrap != null) bootstrap.releaseExternalResources()
    }
    catch {
      case _ =>
    }
    notifyWaiters(reason)
  }


  override def postRestart(reason: Throwable) {
    EventHandler.warning(this, "failTo:" + host + ":" + port)
  }

  private def noConn(): Receive = {
    case _ => self.reply(NoConnectionsLeft)
  }

  private def connect() {
    bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(daemonThreadFactory),
      Executors.newCachedThreadPool(daemonThreadFactory)));
    bootstrap.setPipelineFactory(new PipelineFactory());
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("keepAlive", true)
    // Make a new connection.
    val connectFuture =
      bootstrap.connect(new InetSocketAddress(host, port));
    // Wait until the connection is made successfully
    connectFuture.awaitUninterruptibly()
    if (connectFuture.isSuccess) channel = connectFuture.getChannel
    else throw new IllegalStateException("Channel didnt connect")
    // Get the handler instance to initiate the request.
    handler =
      channel.getPipeline().get(classOf[Handler])
    handler.ref = self
  }

  private def doSend(req: DoozerRequest): Unit = {
    val currentTag = state.tag
    state.tag += 1
    requests += currentTag -> req
    responses += currentTag -> self.senderFuture
    if (!connected) {
      connect()
      connected = true
    }
    handler.send(req.toBuilder.setTag(currentTag).build)
  }


  override protected def receive = {
    case req: DoozerRequest => doSend(req)
    case response: DoozerMsg.Response => {
      requests.remove(response.getTag) match {
        case Some(req) => {
          val msg = DoozerResponse.isOk(response) match {
            case true => req.toResponse(response)
            case false => req.toError(response)
          }
          responses.remove(response.getTag) match {
            case Some(future) => future.get.asInstanceOf[CompletableFuture[Any]].completeWithResult(msg)
            case None => EventHandler.warning(this, "Received a response with tag %d but there was no futute to complete".format(response.getTag))
          }
        }
        case None => EventHandler.warning(this, "Revieved a response with tag %d but there was no request to correlate with".format(response.getTag))
      }
    }
  }


}

import org.jboss.netty.channel._
import akka.actor.ActorRef

class Handler extends SimpleChannelUpstreamHandler {

  @volatile var ref: ActorRef = null
  @volatile var channel: Channel = null

  def send(msg: DoozerMsg.Request) {
    EventHandler.debug(this, "====>sent:" + msg.toString)
    val future = channel.write(msg)
    future.awaitUninterruptibly
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    EventHandler.error(e.getCause, this, "exceptionCaught")
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    EventHandler.debug(this, "===>recieved:" + e.getMessage)
    ref ! e.getMessage
  }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channel = ctx.getChannel
    super.channelOpen(ctx, e)
  }
}

import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.handler.codec.protobuf._

class PipelineFactory extends ChannelPipelineFactory {
  def getPipeline = {
    val p = Channels.pipeline
    p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
    p.addLast("protobufDecoder", new ProtobufDecoder(DoozerMsg.Response.getDefaultInstance()))
    p.addLast("frameEncoder", new LengthFieldPrepender(4))
    p.addLast("protobufEncoder", new ProtobufEncoder())
    p.addLast("handler", new Handler)
    p
  }


}

