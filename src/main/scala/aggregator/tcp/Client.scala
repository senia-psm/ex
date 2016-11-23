package aggregator.tcp

import java.nio.ByteOrder

import aggregator.actor.Aggregator
import aggregator.config.TcpConfig
import aggregator.model.Transaction
import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{Flow, Framing, GraphDSL, Keep, Merge, Sink, Source, Tcp, UnzipWith}
import akka.stream.{Attributes, FlowShape, Materializer}
import akka.util.ByteString
import scodec.Attempt.{Failure, Successful}
import scodec.{Attempt, DecodeResult}

import scala.collection.immutable.Seq
import scala.concurrent.Future


object Client {
  private def decode(byteString: ByteString): Attempt[DecodeResult[Transaction]] = {
    import scodec.bits._

    val byteVector = ByteVector.viewAt(l => byteString.apply(l.toInt), byteString.size)
    Transaction.codec.decode(byteVector.bits)
  }

  private def logDecoded: Flow[Attempt[DecodeResult[Transaction]], Transaction, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      type In = Attempt[DecodeResult[Transaction]]
      val unzip = b.add(UnzipWith[In, In, In, In](i => (i, i, i)))
      val merge = b.add(Merge[Transaction](2))

      val logFails =
        Flow[In]
          .collect { case f: Failure => f }
          .log("Decode failed")
          .addAttributes(Attributes.logLevels(onElement = Logging.ErrorLevel))

      val logRemainder =
        Flow[In]
          .collect { case r@Successful(DecodeResult(_, remainder)) if remainder.nonEmpty => r }
          .log("Not empty remainder")
          .addAttributes(Attributes.logLevels(onElement = Logging.ErrorLevel))

      val logSuccess =
        Flow[In]
          .collect { case r@Successful(DecodeResult(_, remainder)) if remainder.isEmpty => r }
          .log("Parsed")

      val collect = Flow[In].collect { case Successful(DecodeResult(v, _)) => v }

      unzip.out0 ~> logFails ~> Sink.ignore
      unzip.out1 ~> logRemainder ~> collect ~> merge.in(0)
      unzip.out2 ~> logSuccess ~> collect ~> merge.in(1)

      FlowShape(unzip.in, merge.out)
    })

  private[tcp] def aggregateTransactions(aggregator: ActorRef): Sink[ByteString, NotUsed] = {
    Flow[ByteString]
      .via(Framing.lengthField(
        fieldLength = 2,
        fieldOffset = 0,
        maximumFrameLength = 65536,
        byteOrder = ByteOrder.BIG_ENDIAN))
      .map(decode)
      .via(logDecoded)
      .map(tr => Aggregator.TransactionMsg(tr))
      .to(Sink.actorRef(aggregator, Aggregator.Stop("Client flow completed.")))
  }

  def start(tcpConfig: TcpConfig, aggregator: ActorRef)
           (implicit as: ActorSystem, m: Materializer): Future[OutgoingConnection] = {
    val connection = Tcp().outgoingConnection(tcpConfig.upstreamHost, tcpConfig.upstreamPort)

    val sink = aggregateTransactions(aggregator)

    val flow = Flow.fromSinkAndSource(sink, Source(Seq()))

    connection.joinMat(flow)(Keep.left).run()
  }
}
