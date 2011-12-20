/*
 * Created by IntelliJ IDEA.
 * User: sclasen
 * Date: 4/21/11
 * Time: 4:12 PM
 */

package com.heroku.doozer.flange

import doozer.DoozerMsg
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import annotation.tailrec
import util.matching.Regex

import java.util.concurrent.atomic.AtomicInteger
import java.lang.{RuntimeException, Thread}
import collection.mutable.{HashSet, HashMap}
import akka.actor._
import akka.event.Logging
import akka.actor.Status.Failure
import java.util.concurrent.ThreadFactory
import akka.util.Timeout
import doozer.DoozerMsg.Response.Err
import akka.dispatch._
import com.typesafe.config.ConfigFactory


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

  def addConnectionListener(listener: DoozerConnectionListener)

  def removeConnectionListener(listener: DoozerConnectionListener)

  def stop(): Unit


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
  private val system = ActorSystem("flange", ConfigFactory.load().getConfig("flange"))
  private val log = Logging(system, doozerUri)
  private val connection = {
    val state = new ClientState(sk, failoverStrategy(doozerds))
    system.actorOf(Props(creator = () => new ConnectionActor(state)), "connectionActor")
  }

  rev match {
    case Right(RevResponse(current)) => log.info("%s Connected, current rev is %d".format(connection, current))
    case Left(ErrorResponse(code, desc)) => log.error("%s Unable to connect, %s, %s".format(connection, code, desc))
  }

  def stop() {
    system.shutdown()
  }

  private def timeout = Left(ErrorResponse("CLIENT_TIMEOUT", "The operation timed out"))

  private def noConnections = Left(ErrorResponse(allConnectionsFailed, "Attempts to retry the operation at all configured servers failed"))

  private def exception(t: Throwable) = Left(ErrorResponse("DoozerClient Exception", t.getStackTraceString))

  private def retry[T](req: DoozerRequest)(success: PartialFunction[Any, Either[ErrorResponse, T]]): Either[ConnectionFailed, Either[ErrorResponse, T]] = {
    try {
      implicit val reqTimeout = Timeout(req.timeout)
      val f = connection ? req
      f.onFailure {
        case e: Exception => ConnectionFailed
      }
      val resp = Await.result(f, reqTimeout.duration)
      if (success.isDefinedAt(resp)) Right(success(resp))
      else resp match {
        case ErrorResponse(_, desc) if desc equals "permission denied" => {
          Await.result(connection ? AccessRequest(sk), reqTimeout.duration) match {
            case r: AccessResponse => retry(req)(success)
            case er: ErrorResponse => {
              log.error("cant auth")
              Left(ConnectionFailed())
            }
          }
        }
        case e@ErrorResponse(_, _) => Right(Left(e))
        case ConnectionFailed => Left(ConnectionFailed())
        case NoConnectionsLeft => Right(noConnections)
      }
    } catch {
      case e =>
        log.error(e, "error")
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
    implicit val reqTimeout = Timeout(req.timeout)
    (connection ? req).recover {
      case e: Exception => completeFuture(req, responseCallback)(success)
    } onSuccess {
      case response: Any => {
        if (success.isDefinedAt(response)) {
          responseCallback(success.apply(response))
        } else {
          response match {
            case ErrorResponse(_, desc) if desc equals "permission denied" => {
              Await.result(connection ? AccessRequest(sk), reqTimeout.duration) match {
                case r: AccessResponse => completeFuture(req, responseCallback)(success)
                case e: ErrorResponse => {
                  log.error("cant auth")
                  responseCallback(Left(e))
                }
              }
            }
            case e: ErrorResponse => responseCallback(Left(e))
            case NoConnectionsLeft => responseCallback(noConnections)
          }
        }
      }
    }
  }


  def deleteAsync(path: String, rev: Long)(callback: (Either[ErrorResponse, DeleteResponse]) => Unit) {
    completeFuture[DeleteResponse](DeleteRequest(path, rev), callback) {
      case Right(d: DeleteResponse) => Right(d)
    }
  }


  def delete_!(path: String, rev: Long) = delete(path, rev).fold(e => throw new ErrorResponseException(e), r => r)

  def delete(path: String, rev: Long) = complete[DeleteResponse](DeleteRequest(path, rev)) {
    case d: DeleteResponse => Right(d)
  }

  def setAsync(path: String, value: Array[Byte], rev: Long)(callback: (Either[ErrorResponse, SetResponse]) => Unit) {
    completeFuture[SetResponse](SetRequest(path, value, rev), callback) {
      case Right(s@SetResponse(_)) => Right(s)
    }
  }

  def set_!(path: String, value: Array[Byte], rev: Long) = set(path, value, rev).fold(e => throw new ErrorResponseException(e), r => r)


  def set(path: String, value: Array[Byte], rev: Long) = complete[SetResponse](SetRequest(path, value, rev)) {
    case s: SetResponse => Right(s)
  }

  def getAsync(path: String, rev: Long = 0L)(callback: (Either[ErrorResponse, GetResponse]) => Unit) {
    completeFuture[GetResponse](GetRequest(path, rev), callback) {
      case Right(g@GetResponse(_, _)) => Right(g)
    }
  }

  def get(path: String, rev: Long = 0L): Either[ErrorResponse, GetResponse] = complete[GetResponse](GetRequest(path, rev)) {
    case g: GetResponse => Right(g)
  }

  def get_!(path: String, rev: Long = 0L) = get(path, rev).fold(e => throw new ErrorResponseException(e), r => r)

  def revAsync(callback: (Either[ErrorResponse, RevResponse]) => Unit) {
    completeFuture[RevResponse](RevRequest, callback) {
      case Right(r@RevResponse(_)) => Right(r)
    }
  }

  def rev = complete[RevResponse](RevRequest) {
    case r: RevResponse => Right(r)
  }

  def rev_! = rev.fold(e => throw new ErrorResponseException(e), r => r)

  def waitAsync(glob: String, rev: Long, waitFor: Long = Long.MaxValue)(callback: (Either[ErrorResponse, WaitResponse]) => Unit) = {
    completeFuture[WaitResponse](WaitRequest(glob, rev, waitFor), callback) {
      case Right(w@WaitResponse(_, _, _)) => Right(w)
    }
  }

  def wait(glob: String, rev: Long, waitFor: Long = Long.MaxValue) = complete[WaitResponse](WaitRequest(glob, rev, waitFor)) {
    case w: WaitResponse => Right(w)
  }

  def wait_!(glob: String, rev: Long, waitFor: Long = Long.MaxValue) = wait(glob, rev, waitFor).fold(e => throw new ErrorResponseException(e), r => r)

  def statAsync(path: String, rev: Long)(callback: (Either[ErrorResponse, StatResponse]) => Unit) = {
    completeFuture[StatResponse](StatRequest(path, rev), callback) {
      case Right(s@StatResponse(_, _, _)) => Right(s)
    }
  }

  def stat(path: String, rev: Long) = complete[StatResponse](StatRequest(path, rev)) {
    case s: StatResponse => Right(s)
  }


  def stat_!(path: String, rev: Long) = stat(path, rev).fold(e => throw new ErrorResponseException(e), r => r)

  def getdir(dir: String, rev: Long, offset: Int) = complete[GetdirResponse](GetdirRequest(dir, rev, offset)) {
    case g: GetdirResponse => Right(g)
  }

  def getdir_!(dir: String, rev: Long, offset: Int) = getdir(dir, rev, offset).fold(e => throw new ErrorResponseException(e), r => r)

  def getdirAsync(dir: String, rev: Long, offset: Int)(callback: (Either[ErrorResponse, GetdirResponse]) => Unit) = {
    completeFuture[GetdirResponse](GetdirRequest(dir, rev, offset), callback) {
      case Right(g@GetdirResponse(_, _)) => Right(g)
    }
  }

  def walk(glob: String, rev: Long, offset: Int) = complete[WalkResponse](WalkRequest(glob, rev, offset)) {
    case w: WalkResponse => Right(w)
  }

  def walk_!(glob: String, rev: Long, offset: Int) = walk(glob, rev, offset).fold(e => throw new ErrorResponseException(e), r => r)

  def walkAsync(glob: String, rev: Long, offset: Int)(callback: (Either[ErrorResponse, WalkResponse]) => Unit) = {
    completeFuture[WalkResponse](WalkRequest(glob, rev, offset), callback) {
      case Right(w@WalkResponse(_, _, _)) => Right(w)
    }
  }

  def watch(glob: String, rev: Long, waitFor: Long = Long.MaxValue)(callback: (Either[ErrorResponse, WaitResponse]) => Boolean) = {
    def inner(r: Long, either: Either[ErrorResponse, WaitResponse]) {
      if (callback.apply(either)) {
        either match {
          case Left(_) => waitAsync(glob, r, waitFor)(inner(r, _))
          case Right(WaitResponse(_, _, newRev)) => waitAsync(glob, newRev + 1, waitFor)(inner(newRev + 1, _))
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

  def getdir_all_!(dir: String, rev: Long) = getdir_all(dir, rev).fold(e => throw new ErrorResponseException(e), r => r)

  def walk_all_!(glob: String, rev: Long) = walk_all(glob, rev).fold(e => throw new ErrorResponseException(e), r => r)

  def removeConnectionListener(listener: DoozerConnectionListener) = connection ! RemoveListener(listener)

  def addConnectionListener(listener: DoozerConnectionListener) = connection ! AddListener(listener)
}

class ClientState(val secret: String, var hosts: Iterable[String], var tag: Int = 0, val listeners: HashSet[DoozerConnectionListener] = HashSet.empty[DoozerConnectionListener])

class ConnectionFailedException(val host: String, cause: Throwable) extends RuntimeException(cause)

class ErrorResponseException(val resp: ErrorResponse) extends RuntimeException()

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.Channel
import java.util.concurrent.Executors
import java.net.InetSocketAddress
import com.heroku.doozer.flange.Flange._


class ConnectionActor(state: ClientState, connectorFact: => NettyConnector = new NettyProtobufConnector) extends Actor {
  private val log = Logging(context.system, this)
  private var host: String = null
  private var port: Int = 0
  private var requests = new HashMap[Int, DoozerRequest]
  private var responses = new HashMap[Int, ActorRef]
  private var connected = false
  private val connector = connectorFact
  implicit val dispatcher = context.dispatcher

  state.hosts.headOption match {
    case Some(h) => {
      host = h.split(":").apply(0)
      port = h.split(":").apply(1).toInt
      state.hosts = state.hosts.tail
    }
    case None => {
      log.error("No more connections")
    }
  }

  private def notifyWaiters(ex: Throwable) {
    for {
      channel <- responses.values
    } channel ! Failure(ex)
  }

  override def postStop() {
    notifyWaiters(new RuntimeException("Connection actor was stopped"))
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.warning("failed:" + host + ":" + port)
    connected = false
    connector.teardown()
    notifyWaiters(reason)
    notifyDisconnected()
  }


  override def postRestart(reason: Throwable) {
    if (host != null) {
      log.warning("failTo:" + host + ":" + port)
    } else {
      context.become(noConn(), false)
    }
  }

  private def noConn(): Receive = {
    case _ => sender ! NoConnectionsLeft
  }

  private def connect() {
    connector.connect(host, port, self, context.system)
  }

  private def doSend(req: DoozerRequest): Unit = {
    val currentTag = state.tag
    state.tag += 1
    requests += currentTag -> req
    responses += currentTag -> sender
    if (!connected) {
      connect()
      connected = true
      notifyConnected()
    }
    connector.handler().send(req.toBuilder.setTag(currentTag).build)
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
            case Some(channel) => channel ! msg
            case None => log.warning("Received a response with tag %d but there was no futute to complete".format(response.getTag))
          }
        }
        case None => log.warning("Revieved a response with tag %d but there was no request to correlate with".format(response.getTag))
      }
    }

    case AddListener(listener) => state.listeners += listener
    case RemoveListener(listener) => state.listeners.remove(listener)

  }

  private def notifyConnected() {
    state.listeners.map {
      l => Future(l.connected())
    }
  }

  private def notifyDisconnected() {
    state.listeners.map {
      l => Future(l.disconnected())
    }
  }

}


trait NettyConnector {

  def connect(host: String, port: Int, ref: ActorRef, system: ActorSystem)

  def teardown()

  def handler(): Handler

}

class NettyProtobufConnector extends NettyConnector {

  private var _handler: Handler = null
  private var bootstrap: ClientBootstrap = null
  private var channel: Channel = null

  def handler() = _handler

  def teardown() {
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
  }

  def connect(host: String, port: Int, self: ActorRef, system: ActorSystem) {
    bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(daemonThreadFactory),
      Executors.newCachedThreadPool(daemonThreadFactory)));
    _handler = new Handler(self, system)
    bootstrap.setPipelineFactory(new PipelineFactory(_handler));
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

  }
}


import org.jboss.netty.channel._
import akka.actor.ActorRef

class Handler(ref: ActorRef, system: ActorSystem) extends SimpleChannelUpstreamHandler {

  val log = Logging(system, "NettyHandler")
  log.debug("Created NettyHandler")

  @volatile var channel: Channel = null

  def send(msg: DoozerMsg.Request) {
    log.debug("%s====>sent:%s".format(ref, msg.toString))
    val future = channel.write(msg)
    future.awaitUninterruptibly
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    log.error(e.getCause, "exceptionCaught")
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    log.debug("%s====>received:%s".format(ref, e.getMessage))
    ref ! e.getMessage
  }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channel = ctx.getChannel
    super.channelOpen(ctx, e)
  }
}

import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.handler.codec.protobuf._

class PipelineFactory(handler: Handler) extends ChannelPipelineFactory {
  def getPipeline = {
    val p = Channels.pipeline
    p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
    p.addLast("protobufDecoder", new ProtobufDecoder(DoozerMsg.Response.getDefaultInstance()))
    p.addLast("frameEncoder", new LengthFieldPrepender(4))
    p.addLast("protobufEncoder", new ProtobufEncoder())
    p.addLast("handler", handler)
    p
  }


}

