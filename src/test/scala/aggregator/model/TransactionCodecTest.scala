package aggregator.model

import java.time.Instant

import org.scalatest.{FreeSpec, Matchers}
import scodec.Attempt.Successful
import scodec._
import scodec.bits._
import spire.syntax.literals._

class TransactionCodecTest extends FreeSpec with Matchers {
  "A codec" - {
    "should parse transaction bin data" in {
      val bits = hex"0x001a000001588c31117d00044141504c405883333333333300000190".bits

      val expected =
        Transaction(
          timestamp = Instant.parse("2016-11-22T13:19:35.805Z"),
          ticker = Ticker("AAPL"),
          price = 98.05,
          size = ui"400"
        )

      Transaction.codec.decode(bits) should be(Successful(DecodeResult(expected, BitVector.empty)))
    }

    "should encode transaction" in {
      val transaction =
        Transaction(
          timestamp = Instant.parse("2016-11-22T13:19:37.738Z"),
          ticker = Ticker("MSFT"),
          price = 92.55,
          size = ui"1700"
        )

      val expected =
        hex"0x001a000001588c31190a00044d5346544057233333333333000006a4".bits

      Transaction.codec.encode(transaction) should be(Successful(expected))
    }

    "should decode transaction bin data with remainder" in {
      val remainder = hex"01234567890abcde".bits
      val bits = hex"0x001a000001588c31227c0004474f4f4740589333333333330000157c".bits ++ remainder

      val expected =
        Transaction(
          timestamp = Instant.parse("2016-11-22T13:19:40.156Z"),
          ticker = Ticker("GOOG"),
          price = 98.3,
          size = ui"5500"
        )

      Transaction.codec.decode(bits) should be(Successful(DecodeResult(expected, remainder)))
    }

    "should encode and decode data without loses" in {
      val bits = hex"0x0019000001588c3123540003535059405b700000000000000001f4".bits

      val expected =
        Transaction(
          timestamp = Instant.parse("2016-11-22T13:19:40.372Z"),
          ticker = Ticker("SPY"),
          price = 109.75,
          size = ui"500"
        )

      Transaction.codec.decode(bits) should be(Successful(DecodeResult(expected, BitVector.empty)))
      Transaction.codec.encode(expected) should be(Successful(bits))
    }
  }
}
