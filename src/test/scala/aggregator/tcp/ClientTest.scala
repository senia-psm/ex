package aggregator.tcp

import java.time.Instant

import aggregator.actor.Aggregator.{Stop, TransactionMsg}
import aggregator.model.{Ticker, Transaction}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.ByteString
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}
import spire.syntax.literals._

class ClientTest
  extends TestKit(ActorSystem("ClientTest"))
    with FreeSpecLike
    with Matchers
    with ImplicitSender
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()

  "Client flow" - {
    "should stop the aggregator when closed" in {
      val aggregator = TestProbe()

      val sink = Client.aggregateTransactions(aggregator.testActor)

      sink.runWith(Source.empty)

      aggregator.expectMsgClass(classOf[Stop])
    }

    "should send a parsed message to the aggregator" in {
      val aggregator = TestProbe()

      val sink = Client.aggregateTransactions(aggregator.testActor)

      val bytes = ByteString(0, 26, 0, 0, 1, 88, -116, 49, 36, -109, 0, 4, 65, 65, 80, 76, 64, 91, 76, -52, -52, -52, -52, -51, 0, 0, 6, -92)
      sink.runWith(Source.single(bytes))

      aggregator.expectMsg(
        TransactionMsg(
          Transaction(Instant.parse("2016-11-22T13:19:40.691Z"), Ticker("AAPL"), 109.2, ui"1700")
        )
      )

      aggregator.expectMsgClass(classOf[Stop])
    }

    "should send multiple parsed messages to the aggregator" in {
      val aggregator = TestProbe()

      val sink = Client.aggregateTransactions(aggregator.testActor)

      val bytes = Vector(
        ByteString(0),
        ByteString(26, 0, 0, 1, 88, -116, 49, 17, 125, 0, 4, 65, 65, 80, 76, 64, 88, -125, 51, 51, 51, 51, 51, 0, 0, 1, -112,
          0, 26, 0, 0, 1, 88, -116, 49, 25, 10, 0, 4, 77, 83, 70, 84, 64, 87, 35, 51, 51, 51, 51, 51, 0, 0, 6, -92,
          0, 26, 0, 0, 1, 88, -116, 49, 34, 124, 0, 4),
        ByteString(71, 79, 79, 71, 64, 88, -109, 51, 51, 51, 51, 51, 0, 0, 21, 124,
          0, 25, 0, 0, 1, 88, -116, 49, 35, 84, 0, 3, 83, 80, 89, 64, 91, 112, 0, 0, 0, 0, 0, 0, 0, 1, -12,
          0, 26, 0, 0, 1, 88, -116, 49, 36, -109, 0, 4, 65, 65, 80, 76, 64, 91, 76, -52, -52, -52, -52, -51, 0, 0),
        ByteString(6, -92))
      sink.runWith(Source(bytes))

      aggregator.expectMsg(
        TransactionMsg(Transaction(Instant.parse("2016-11-22T13:19:35.805Z"), Ticker("AAPL"), 98.05, ui"400"))
      )
      aggregator.expectMsg(
        TransactionMsg(Transaction(Instant.parse("2016-11-22T13:19:37.738Z"), Ticker("MSFT"), 92.55, ui"1700"))
      )
      aggregator.expectMsg(
        TransactionMsg(Transaction(Instant.parse("2016-11-22T13:19:40.156Z"), Ticker("GOOG"), 98.3, ui"5500"))
      )
      aggregator.expectMsg(
        TransactionMsg(Transaction(Instant.parse("2016-11-22T13:19:40.372Z"), Ticker("SPY"), 109.75, ui"500"))
      )
      aggregator.expectMsg(
        TransactionMsg(Transaction(Instant.parse("2016-11-22T13:19:40.691Z"), Ticker("AAPL"), 109.2, ui"1700"))
      )

      aggregator.expectMsgClass(classOf[Stop])
    }
  }
}
