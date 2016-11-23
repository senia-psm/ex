package aggregator.tcp

import aggregator.actor.Aggregator._
import aggregator.config.TcpConfig
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl.Tcp.ServerBinding
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.stream.{Materializer, OverflowStrategy}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object Server {
  def start(tcpConfig: TcpConfig, aggregator: ActorRef)
           (implicit as: ActorSystem, m: Materializer, ec: ExecutionContext): Future[ServerBinding] = {
    val serverStarted = Promise[ServerBinding]()

    val connections =
      Tcp().bind(tcpConfig.bindHost, tcpConfig.bindPort)
          .mapMaterializedValue(serverStarted.completeWith)

    connections
      .runForeach { connection =>
        val source = candlesSource(tcpConfig, aggregator)

        val flow = Flow.fromSinkAndSource(Sink.ignore, source)

        connection.handleWith(flow)
      }
      .onComplete{
        case Success(_) =>
          aggregator ! Stop("Server Binding closed with success status")
        case Failure(e) => aggregator ! Stop(s"Server Binding failed: $e")
      }

    serverStarted.future
  }

  private[tcp] def candlesSource(tcpConfig: TcpConfig, aggregator: ActorRef): Source[ByteString, Unit] = {
    Source
      .actorRef[Event](tcpConfig.outputBuffer, OverflowStrategy.dropHead)
      .mapMaterializedValue(aggregator ! Register(_))
      .takeWhile(_ != End)
      .collect {
        case Candles(cs) => cs
      }
      .mapConcat(identity)
      .map(tcpConfig.formatCandle)
      .map(_ + tcpConfig.outputSeparator)
      .map(ByteString(_))
  }
}