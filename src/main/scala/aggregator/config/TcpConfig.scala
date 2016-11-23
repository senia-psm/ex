package aggregator.config

import java.time.Instant
import java.time.format.DateTimeFormatter

import aggregator.model.{Candle, Ticker}
import com.typesafe.config.Config


trait TcpConfig {
  def upstreamHost: String
  def upstreamPort: Int
  def bindHost: String
  def bindPort: Int
  def outputBuffer: Int
  def outputSeparator: String

  def formatTicker(ticker: Ticker): String = {
    ticker
      .name
      .replace("\\", "\\\\")
      .replace("/", "\\/")
      .replace("\"", "\\\"")
  }

  def formatInstant(instant: Instant): String = {
    DateTimeFormatter.ISO_INSTANT.format(instant)
  }

  def formatCandle(candle: Candle): String = {
    import candle._
    s"""{ "ticker": "${formatTicker(ticker)}", "timestamp": "$timestamp", "open": $open, "high": $high, "low": $low, "close": $close, "volume": $volume }"""
  }
}

object TcpConfig {
  def fromConfig(config: Config): TcpConfig = {
    new TcpConfig {
      val upstreamHost: String = config.getString("upstream.host")
      val upstreamPort: Int = config.getInt("upstream.port")
      val bindHost: String = config.getString("bind.host")
      val bindPort: Int = config.getInt("bind.port")
      val outputBuffer: Int = config.getInt("output.buffer")
      val outputSeparator: String = config.getString("output.separator")
    }
  }
}