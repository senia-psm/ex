package aggregator.actor

import java.time.Instant
import java.time.temporal.{ChronoUnit, TemporalUnit}

import aggregator.config.AggregatorConfig
import aggregator.model.{Candle, Ticker, Transaction}
import akka.actor.{Actor, ActorRef, Terminated}
import akka.event.Logging

import scala.collection.immutable.Seq

class Aggregator(implicit config: AggregatorConfig) extends Actor {
  val log = Logging(context.system, this)

  import Aggregator._

  override def receive: Receive = behaviour(State(), Set.empty)

  def sendPushHistory(): Unit = {
    import context.dispatcher

    import concurrent.duration._

    val truncateTo: TemporalUnit = config.truncateTo
    val truncateSeconds = truncateTo.getDuration.getSeconds

    val now = Instant.now()
    val secondsFromLastOpen = now.truncatedTo(truncateTo).until(now, ChronoUnit.SECONDS)
    val delay = (truncateSeconds - secondsFromLastOpen).seconds + config.historyDelayGap
    context.system.scheduler.scheduleOnce(delay, self, PushHistory)
  }

  override def preStart(): Unit = {
    log.debug("Starting")
    sendPushHistory()
  }

  private def sendCandles(freshCandles: Option[Seq[Candle]], listeners: Set[ActorRef]) = {
    log.debug("Sending candles: {}", freshCandles)
    for {
      candles <- freshCandles.seq
      listener <- listeners
    } listener ! Candles(candles)
  }

  def behaviour(state: State, listeners: Set[ActorRef]): Receive = {
    case stop@Stop(_) =>
      log.info("Stop message received: {}", stop)
      listeners.foreach(_ ! End)
      context.stop(self)

    case PushHistory =>
      val openTime = Instant.now().truncatedTo(config.truncateTo)
      val (newState, freshCandles) = state.pushHistory(openTime)
      sendCandles(freshCandles, listeners)
      sendPushHistory()

      context.become(behaviour(newState, listeners))

    case TransactionMsg(tr) =>
      log.debug("Transaction: {}", tr)
      val (newState, freshCandles) = state.add(tr)
      sendCandles(freshCandles, listeners)

      context.become(behaviour(newState, listeners))

    case Register(listener) =>
      log.debug("New listener: {}", listener)
      context.watch(listener)
      listener ! Candles(state.history.flatMap(_.candles))
      context.become(behaviour(state, listeners + listener))

    case Terminated(listener) =>
      log.debug("Listener terminated: {}", listener)
      context.become(behaviour(state, listeners - listener))
  }
}

object Aggregator {
  sealed trait Event
  case object End extends Event
  case class Candles(candles: Seq[Candle]) extends Event

  sealed trait Message
  case class TransactionMsg(transaction: Transaction) extends Message
  case class Register(listener: ActorRef) extends Message
  case object PushHistory extends Message
  case class Stop(reason: String) extends Message


  case class History(timestamp: Instant, candles: Seq[Candle])

  case class State(history: Seq[History] = Vector.empty,
                   currentOpenTime: Option[Instant] = None,
                   current: Map[Ticker, Candle] = Map.empty) {

    private def cropHistory(history: Seq[History])(implicit config: AggregatorConfig): Seq[History] = {
      history.drop(math.max(0, history.size - config.historySize))
    }

    def pushHistory(currentTime: Instant)(implicit config: AggregatorConfig): (State, Option[Seq[Candle]]) = {
      val openTime = currentTime.truncatedTo(config.truncateTo)

      currentOpenTime match {
        case None =>
          this.copy(currentOpenTime = Some(openTime)) -> None

        case Some(cot) if cot.isBefore(openTime) =>
          val freshCandles = current.values.toVector
          val updatedHistory = history :+ History(cot, freshCandles)
          State(
            history = cropHistory(updatedHistory),
            currentOpenTime = Some(openTime),
            Map.empty) -> Some(freshCandles)

        case _ =>
          this -> None
      }
    }

    private def addUnsafe(transaction: Transaction)
                         (implicit config: AggregatorConfig): State = {
      import transaction._

      val openTime = timestamp.truncatedTo(config.truncateTo)

      val updatedCandle = current.get(ticker) match {
        case None =>
          Candle(openTime, ticker, price, price, price, price, size)

        case Some(candle) =>
          candle.copy(
            high = math.max(candle.high, price),
            low = math.min(candle.low, price),
            close = price,
            volume = candle.volume + size
          )
      }

      this.copy(current = current.updated(ticker, updatedCandle))
    }

    def add(transaction: Transaction)(implicit config: AggregatorConfig): (State, Option[Seq[Candle]]) = {
      val (state, freshCandles) = pushHistory(transaction.timestamp)

      state.addUnsafe(transaction) -> freshCandles
    }
  }
}