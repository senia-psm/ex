package aggregator.tcp

import java.time.Instant

import aggregator.actor.Aggregator.{Candles, End, Register}
import aggregator.config.TcpConfig
import aggregator.model.{Candle, Ticker}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}
import spire.syntax.literals._

class ServerTest
  extends TestKit(ActorSystem("ServerTest"))
    with FreeSpecLike
    with Matchers
    with ImplicitSender
    with BeforeAndAfterAll
    with ScalaFutures {

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  val config = new TcpConfig {
    def upstreamHost: String = ???
    override def upstreamPort: Int = ???
    override def bindHost: String = ???
    override def bindPort: Int = ???

    override def outputSeparator: String = "\n"
    override def outputBuffer: Int = 10
  }

  implicit val materializer = ActorMaterializer()

  "Server flow" - {
    "should send a message" in {
      val probe = TestProbe()
      val source = Server.candlesSource(config, probe.testActor)

      val futureBytes = source.runWith(Sink.reduce[ByteString](_ ++ _))

      val Register(actorRef) = probe.expectMsgClass(classOf[Register])

      val candle = Candle(Instant.parse("2016-11-22T13:06:00Z"), Ticker("SPY"), 96.05, 106.55, 96.05, 106.55, ui"12900")

      actorRef ! Candles(Vector(candle))
      actorRef ! End

      futureBytes.futureValue should be(ByteString(config.formatCandle(candle) + config.outputSeparator))
    }

    "should send multiple messages" in {

      val probe = TestProbe()
      val receiver = TestProbe()
      val source = Server.candlesSource(config, probe.testActor)

      source.runWith(Sink.actorRef(receiver.testActor, "done"))

      val Register(actorRef) = probe.expectMsgClass(classOf[Register])

      val candle1 = Candle(Instant.parse("2016-11-22T13:06:00Z"), Ticker("SPY"), 96.05, 106.55, 96.05, 106.55, ui"12900")
      val candle2 = Candle(Instant.parse("2029-12-22T13:11:00Z"), Ticker("abc"), 196.05, 206.55, 6.05, 76.55, ui"12901")
      val candle3 = Candle(Instant.parse("2032-01-03T11:56:00Z"), Ticker("xyZ"), 96.05, 206.55, 0.01, 106.65, ui"12907")

      def format(candle: Candle) = ByteString(config.formatCandle(candle) + config.outputSeparator)

      actorRef ! Candles(Vector(candle1))
      receiver.expectMsg(format(candle1))

      actorRef ! Candles(Vector(candle2))
      receiver.expectMsg(format(candle2))

      actorRef ! Candles(Vector(candle3))
      receiver.expectMsg(format(candle3))

      actorRef ! End
      receiver.expectMsg("done")
    }
  }

}
