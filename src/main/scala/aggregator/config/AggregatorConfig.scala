package aggregator.config

import java.time.temporal.{ChronoUnit, TemporalUnit}

import com.typesafe.config.Config

import scala.concurrent.duration._

trait AggregatorConfig {
  def truncateTo: TemporalUnit
  def historySize: Int
  def historyDelayGap: FiniteDuration
}

object AggregatorConfig {
  def fromConfig(config: Config): AggregatorConfig = {
    new AggregatorConfig {
      val truncateTo: TemporalUnit = ChronoUnit.valueOf(config.getString("truncateTo.chronoUnit"))
      val historySize: Int = config.getInt("history.size")
      val historyDelayGap: FiniteDuration = config.getDuration("history.delayGap").getSeconds.seconds
    }
  }
}


