package com.lightbend.lagom.internal.broker.pubsub

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.concurrent.duration._

sealed trait PubsubConfig {
  /** The GCP project id of the Pub/Sub */
  def projectId: String

  /** The path to the service account */
  def serviceAccountPath: Option[String]

  /** Host of the Google Pub/Sub emulator */
  def emulatorHost: Option[String]
}

object PubsubConfig {
  def apply(conf: Config): PubsubConfig =
    new PubsubConfigImpl(conf.getConfig("lagom.broker.pubsub"))

  private final class PubsubConfigImpl(conf: Config) extends PubsubConfig {
    override val projectId: String = conf.getString("project-id")
    override val serviceAccountPath: Option[String] =
      if (conf.hasPath("service-account-path")) Some(conf.getString("service-account-path")) else None
    override val emulatorHost: Option[String] =
      if (conf.hasPath("emulator-host")) Some(conf.getString("emulator-host")) else None
  }

}

sealed trait ClientConfig {
  def minBackoff: FiniteDuration

  def maxBackoff: FiniteDuration

  def randomBackoffFactor: Double
}

object ClientConfig {

  private[pubsub] class ClientConfigImpl(conf: Config) extends ClientConfig {
    override val minBackoff: FiniteDuration =
      conf.getDuration("failure-exponential-backoff.min", TimeUnit.MILLISECONDS).millis
    override val maxBackoff: FiniteDuration =
      conf.getDuration("failure-exponential-backoff.max", TimeUnit.MILLISECONDS).millis
    override val randomBackoffFactor: Double = conf.getDouble("failure-exponential-backoff.random-factor")
  }

}

sealed trait ProducerConfig extends ClientConfig {
  def role: Option[String]
}

object ProducerConfig {
  def apply(conf: Config): ProducerConfig =
    new ProducerConfigImpl(conf.getConfig("lagom.broker.pubsub.client.producer"))

  private class ProducerConfigImpl(conf: Config) extends ClientConfig.ClientConfigImpl(conf) with ProducerConfig {

    override val role: Option[String] = conf.getString("role") match {
      case "" => None
      case other => Some(other)
    }
  }

}

sealed trait ConsumerConfig extends ClientConfig {
  /**
    * The maximum time after a subscriber receives a message before
    * the subscriber should acknowledge the message (seconds)
    */
  def ackDeadline: Int

  /**
    * The time (milliseconds) between Pub/Sub pulling request
    */
  def pullingInterval: Int

  /**
    * The maximum buffer size for the messages stream
    */
  def offsetBuffer: Int

  /**
    * The number of messages to group before acknowledging
    */
  def batchingSize: Int

  /**
    * The time to group messages before acknowledging
    */
  def batchingInterval: FiniteDuration
}

object ConsumerConfig {
  def apply(conf: Config): ConsumerConfig =
    new ConsumerConfigImpl(conf.getConfig("lagom.broker.pubsub.client.consumer"))

  private final class ConsumerConfigImpl(conf: Config) extends ClientConfig.ClientConfigImpl(conf) with ConsumerConfig {

    override val ackDeadline: Int = conf.getInt("ack-deadline")

    override val pullingInterval: Int = conf.getInt("pulling-interval")

    override def offsetBuffer: Int = conf.getInt("offset-buffer")

    override val batchingSize: Int = conf.getInt("batching-size")

    override val batchingInterval: FiniteDuration = {
      val interval = conf.getDuration("batching-interval")
      FiniteDuration(interval.toMillis(), TimeUnit.MILLISECONDS)
    }
  }

}

