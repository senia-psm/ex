package aggregator

import aggregator.actor.Aggregator
import aggregator.actor.Aggregator.Stop
import aggregator.config.{AggregatorConfig, TcpConfig}
import aggregator.tcp.{Client, Server}
import akka.actor.{Actor, ActorSystem, Props, Terminated}
import akka.event.Logging
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.OutgoingConnection
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


object Main {

  lazy val config: Config = ConfigFactory.load()

  lazy val tcpConfig: TcpConfig = TcpConfig.fromConfig(config.getConfig("tcp"))

  implicit lazy val aggregatorConfig: AggregatorConfig = AggregatorConfig.fromConfig(config.getConfig("aggregator"))

  def main(args: Array[String]): Unit = {
    tcpConfig
    aggregatorConfig

    object RootActor {
      sealed trait Message
      case object Start extends Message
    }

    class RootActor extends Actor {
      val log = Logging(context.system, this)

      import RootActor._

      override def preStart(): Unit = {
        log.debug("Starting")
        self ! Start
      }

      override def receive: Receive = {
        case Start =>

          implicit val materializer = ActorMaterializer()

          val aggregator =
            context.system.actorOf(Props(new Aggregator), "aggregator")

          context.watch(aggregator)

          implicit val system = context.system
          import context.dispatcher

          Client.start(tcpConfig, aggregator).onComplete {
            case Success(oc) =>
              log.info(s"Established outgoing connection with ${oc.remoteAddress}. Local address: ${oc.localAddress}")
            case Failure(e) =>
              log.error(e, "Client startup failed")
              aggregator ! Stop("Client startup failed")
          }

          Server.start(tcpConfig, aggregator).onComplete {
            case Success(sb) =>
              log.info(s"Server started at ${sb.localAddress}")
            case Failure(e) =>
              log.error(e, "Server startup failed")
              aggregator ! Stop("Server startup failed")
          }

          context.become {
            case Terminated(`aggregator`) =>
              log.info("Aggregator is terminated. Stopping.")
              context.system.terminate()
          }
      }
    }

    val system = ActorSystem("main")

    system.actorOf(Props(new RootActor))

    Await.ready(system.whenTerminated, Duration.Inf)
  }
}
