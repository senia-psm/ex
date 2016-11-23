package aggregator.model

import java.time.Instant

import scodec.Codec
import spire.math.UInt


case class Transaction(timestamp: Instant, ticker: Ticker, price: Double, size: UInt)

object Transaction {
  val codec: Codec[Transaction] = {
    import scodec._
    import codecs._
    import scodec.interop.spire._

    val timestamp = int64.xmap[Instant](Instant.ofEpochMilli, _.toEpochMilli)
    val ticker = variableSizeBytes(uint16, utf8).xmap[Ticker](Ticker, _.name)
    val price = double
    val size = suint32

    val codec = (timestamp :: ticker :: price :: size).as[Transaction]
    variableSizeBytes(uint16, codec)
  }
}

