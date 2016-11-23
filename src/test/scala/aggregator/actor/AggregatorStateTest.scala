package aggregator.actor

import java.time.Instant
import java.time.temporal.ChronoUnit

import aggregator.actor.Aggregator.{History, State}
import aggregator.config.AggregatorConfig
import aggregator.model.{Candle, Ticker, Transaction}
import org.scalatest.{FreeSpec, Matchers}
import spire.syntax.literals._

import scala.collection.immutable._

class AggregatorStateTest extends FreeSpec with Matchers {
  implicit val aggregatorConfig = new AggregatorConfig {
    val truncateTo = ChronoUnit.MINUTES
    val historySize = 10
    def historyDelayGap = ???
  }

  "Aggregator state" - {
    "`pushHistory' method" - {
      "should not affect state with the same current frame" in {
        val openTimeInit = Instant.parse("2016-11-22T13:29:00Z")
        val stateInit = State(
          history = Seq(
            History(Instant.parse("2016-11-22T13:20:00Z"),
              Seq(Candle(Instant.parse("2016-11-22T13:20:00Z"), Ticker("abc"), 2.01, 8.99, 1.03, 5.04, ui"454"))),
            History(Instant.parse("2016-11-22T13:21:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:22:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:23:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:24:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:25:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:26:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:27:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:28:00Z"), Seq())
          ),
          currentOpenTime = Some(openTimeInit),
          current = Map(Ticker("AAPL") -> Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"))
        )

        val (state, toSend) = stateInit.pushHistory(Instant.parse("2016-11-22T13:29:59Z"))

        state should be(stateInit)

        toSend should be(None)
      }

      "should push current candles to history on new time frame" in {
        val openTimeInit = Instant.parse("2016-11-22T13:29:00Z")
        val stateInit = State(
          history = Seq(
            History(Instant.parse("2016-11-22T13:20:00Z"),
              Seq(Candle(Instant.parse("2016-11-22T13:20:00Z"), Ticker("abc"), 2.01, 8.99, 1.03, 5.04, ui"454"))),
            History(Instant.parse("2016-11-22T13:21:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:22:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:23:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:24:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:25:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:26:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:27:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:28:00Z"), Seq())
          ),
          currentOpenTime = Some(openTimeInit),
          current = Map(Ticker("AAPL") -> Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"))
        )

        val (state, toSend) = stateInit.pushHistory(Instant.parse("2016-11-22T13:30:01Z"))

        state should be(State(
          history = Seq(
            History(Instant.parse("2016-11-22T13:20:00Z"),
              Seq(Candle(Instant.parse("2016-11-22T13:20:00Z"), Ticker("abc"), 2.01, 8.99, 1.03, 5.04, ui"454"))),
            History(Instant.parse("2016-11-22T13:21:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:22:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:23:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:24:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:25:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:26:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:27:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:28:00Z"), Seq()),
            History(openTimeInit, Seq(Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404")))
          ),
          currentOpenTime = Some(Instant.parse("2016-11-22T13:30:00Z")),
          current = Map.empty
        ))

        toSend should be(Some(Seq(Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"))))
      }

      "should ensure history max size" in {
        val openTimeInit = Instant.parse("2016-11-22T13:29:00Z")
        val stateInit = State(
          history = Seq(
            History(Instant.parse("2016-11-22T13:19:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:20:00Z"),
              Seq(Candle(Instant.parse("2016-11-22T13:20:00Z"), Ticker("abc"), 2.01, 8.99, 1.03, 5.04, ui"454"))),
            History(Instant.parse("2016-11-22T13:21:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:22:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:23:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:24:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:25:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:26:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:27:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:28:00Z"), Seq())
          ),
          currentOpenTime = Some(openTimeInit),
          current = Map(Ticker("AAPL") -> Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"))
        )

        val (state, toSend) = stateInit.pushHistory(Instant.parse("2016-11-22T13:30:01Z"))

        state should be(State(
          history = Seq(
            History(Instant.parse("2016-11-22T13:20:00Z"),
              Seq(Candle(Instant.parse("2016-11-22T13:20:00Z"), Ticker("abc"), 2.01, 8.99, 1.03, 5.04, ui"454"))),
            History(Instant.parse("2016-11-22T13:21:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:22:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:23:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:24:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:25:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:26:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:27:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:28:00Z"), Seq()),
            History(openTimeInit, Seq(Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404")))
          ),
          currentOpenTime = Some(Instant.parse("2016-11-22T13:30:00Z")),
          current = Map.empty
        ))

        toSend should be(Some(Seq(Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"))))
      }
    }

    "`add' method" - {
      "should aggregate a single transaction" in {
        val transaction = Transaction(
          timestamp = Instant.parse("2016-11-22T13:19:35.805Z"),
          ticker = Ticker("AAPL"),
          price = 98.05,
          size = ui"400"
        )

        val openTime = Instant.parse("2016-11-22T13:19:00Z")
        State().add(transaction) should be(State(
          history = Seq.empty,
          currentOpenTime = Some(openTime),
          current = Map(Ticker("AAPL") -> Candle(openTime, Ticker("AAPL"), 98.05, 98.05, 98.05, 98.05, ui"400"))
        ) -> None)
      }

      "expects transactions to be ordered by timestamp" ignore {
        // TODO check protocol specification
      }

      "should aggregate multiple transaction" in {
        val t1 = Transaction(Instant.parse("2016-11-22T13:19:01Z"), Ticker("AAPL"), 1.01, ui"101")
        val t2 = Transaction(Instant.parse("2016-11-22T13:19:02Z"), Ticker("AAPL"), 9.99, ui"101")
        val t2_2 = Transaction(Instant.parse("2016-11-22T13:19:02Z"), Ticker("abc"), 8.88, ui"111")
        val t3 = Transaction(Instant.parse("2016-11-22T13:19:03Z"), Ticker("AAPL"), 0.03, ui"101")
        val t4 = Transaction(Instant.parse("2016-11-22T13:19:04Z"), Ticker("AAPL"), 4.04, ui"101")

        val openTime = Instant.parse("2016-11-22T13:19:00Z")

        val state0 = State()
        val (state1, toSend1) = state0.add(t1)

        state1 should be(State(
          history = Seq.empty,
          currentOpenTime = Some(openTime),
          current = Map(Ticker("AAPL") -> Candle(openTime, Ticker("AAPL"), 1.01, 1.01, 1.01, 1.01, ui"101"))
        ))
        toSend1 should be(None)

        val (state2, toSend2) = state1.add(t2)

        state2 should be(State(
          history = Seq.empty,
          currentOpenTime = Some(openTime),
          current = Map(Ticker("AAPL") -> Candle(openTime, Ticker("AAPL"), 1.01, 9.99, 1.01, 9.99, ui"202"))
        ))
        toSend2 should be(None)

        val (state3, toSend3) = state2.add(t2_2)

        state3 should be(State(
          history = Seq.empty,
          currentOpenTime = Some(openTime),
          current = Map(
            Ticker("AAPL") -> Candle(openTime, Ticker("AAPL"), 1.01, 9.99, 1.01, 9.99, ui"202"),
            Ticker("abc") -> Candle(openTime, Ticker("abc"), 8.88, 8.88, 8.88, 8.88, ui"111")
          )
        ))
        toSend3 should be(None)

        val (state4, toSend4) = state3.add(t3)

        state4 should be(State(
          history = Seq.empty,
          currentOpenTime = Some(openTime),
          current = Map(
            Ticker("AAPL") -> Candle(openTime, Ticker("AAPL"), 1.01, 9.99, 0.03, 0.03, ui"303"),
            Ticker("abc") -> Candle(openTime, Ticker("abc"), 8.88, 8.88, 8.88, 8.88, ui"111")
          )
        ))
        toSend4 should be(None)

        val (state5, toSend5) = state4.add(t4)

        state5 should be(State(
          history = Seq.empty,
          currentOpenTime = Some(openTime),
          current = Map(
            Ticker("AAPL") -> Candle(openTime, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"),
            Ticker("abc") -> Candle(openTime, Ticker("abc"), 8.88, 8.88, 8.88, 8.88, ui"111")
          )
        ))
        toSend5 should be(None)
      }

      "should push current state to history on new time frame" in {
        val openTimeInit = Instant.parse("2016-11-22T13:19:00Z")
        val stateInit = State(
          history = Seq.empty,
          currentOpenTime = Some(openTimeInit),
          current = Map(
            Ticker("AAPL") -> Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"),
            Ticker("abc") -> Candle(openTimeInit, Ticker("abc"), 8.88, 8.88, 8.88, 8.88, ui"111")
          )
        )

        val t = Transaction(Instant.parse("2016-11-22T13:20:01Z"), Ticker("AAPL"), 2.02, ui"123")

        val (updatedState, Some(toSend)) = stateInit.add(t)

        toSend should have(length(2))
        toSend.toSet should be(Set(
          Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"),
          Candle(openTimeInit, Ticker("abc"), 8.88, 8.88, 8.88, 8.88, ui"111")
        ))

        updatedState.history should have(length(1))

        updatedState.history.head.timestamp should be(openTimeInit)
        updatedState.history.head.candles should have(length(2))
        updatedState.history.head.candles.toSet should be(Set(
          Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"),
          Candle(openTimeInit, Ticker("abc"), 8.88, 8.88, 8.88, 8.88, ui"111")
        ))

        val openTime = Instant.parse("2016-11-22T13:20:00Z")
        updatedState.currentOpenTime should be(Some(openTime))

        updatedState.current should be(Map(
          Ticker("AAPL") -> Candle(openTime, Ticker("AAPL"), 2.02, 2.02, 2.02, 2.02, ui"123")
        ))
      }

      "should ensure history max size" in {
        val openTimeInit = Instant.parse("2016-11-22T13:29:00Z")
        val stateInit = State(
          history = Seq(
            History(Instant.parse("2016-11-22T13:19:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:20:00Z"),
              Seq(Candle(Instant.parse("2016-11-22T13:20:00Z"), Ticker("abc"), 2.01, 8.99, 1.03, 5.04, ui"454"))),
            History(Instant.parse("2016-11-22T13:21:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:22:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:23:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:24:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:25:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:26:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:27:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:28:00Z"), Seq())
          ),
          currentOpenTime = Some(openTimeInit),
          current = Map(Ticker("AAPL") -> Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404"))
        )

        val t = Transaction(Instant.parse("2016-11-22T13:30:01Z"), Ticker("AAPL"), 2.02, ui"123")

        val (state, Some(toSend)) = stateInit.add(t)

        toSend should be(Seq(Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404")))

        val openTime = Instant.parse("2016-11-22T13:30:00Z")

        state should be(State(
          history = Seq(
            History(Instant.parse("2016-11-22T13:20:00Z"),
              Seq(Candle(Instant.parse("2016-11-22T13:20:00Z"), Ticker("abc"), 2.01, 8.99, 1.03, 5.04, ui"454"))),
            History(Instant.parse("2016-11-22T13:21:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:22:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:23:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:24:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:25:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:26:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:27:00Z"), Seq()),
            History(Instant.parse("2016-11-22T13:28:00Z"), Seq()),
            History(openTimeInit, Seq(Candle(openTimeInit, Ticker("AAPL"), 1.01, 9.99, 0.03, 4.04, ui"404")))
          ),
          currentOpenTime = Some(openTime),
          current = Map(Ticker("AAPL") -> Candle(openTime, Ticker("AAPL"), 2.02, 2.02, 2.02, 2.02, ui"123"))
        ))
      }
    }
  }
}
