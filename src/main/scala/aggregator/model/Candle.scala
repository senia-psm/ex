package aggregator.model

import java.time.Instant

import spire.math.UInt

case class Candle(timestamp: Instant,
                  ticker: Ticker,
                  open: Double,
                  high: Double,
                  low: Double,
                  close: Double,
                  volume: UInt)
