package com.elegantmonkeys.lagom.internal.broker.pubsub

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.concurrent.duration._

sealed trait PubsubConfig {
  /** The GCP project id of the Pub/Sub */
  def projectId: String

  /** The path to the service account */
  def serviceAccountPath: String
}

object PubsubConfig {
  def apply(conf: Config): PubsubConfig =
    new PubsubConfigImpl(conf.getConfig("lagom.broker.pubsub"))

  private final class PubsubConfigImpl(conf: Config) extends PubsubConfig {
    override val projectId: String = conf.getString("project-id")
    override val serviceAccountPath: String = conf.getString("service-account-path")
  }

}

sealed trait ProducerConfig {
  def minBackoff: FiniteDuration

  def maxBackoff: FiniteDuration

  def randomBackoffFactor: Double

  def role: Option[String]
}

object ProducerConfig {
  def apply(conf: Config): ProducerConfig =
    new ProducerConfigImpl(conf.getConfig("lagom.broker.pubsub.client.producer"))

  private class ProducerConfigImpl(conf: Config) extends ProducerConfig {

    override val role: Option[String] = conf.getString("role") match {
      case "" => None
      case other => Some(other)
    }

    override val minBackoff: FiniteDuration =
      conf.getDuration("failure-exponential-backoff.min", TimeUnit.MILLISECONDS).millis

    override val maxBackoff: FiniteDuration =
      conf.getDuration("failure-exponential-backoff.max", TimeUnit.MILLISECONDS).millis

    override val randomBackoffFactor: Double = conf.getDouble("failure-exponential-backoff.random-factor")
  }

}

sealed trait ConsumerConfig {
  /** The name of the subscription that will be created to pull messages */
  def subscriptionName: String

  /**
    * The maximum time after a subscriber receives a message before
    * the subscriber should acknowledge the message (seconds)
    */
  def ackDeadline: Int
}

object ConsumerConfig {
  def apply(conf: Config): ConsumerConfig =
    new ConsumerConfigImpl(conf.getConfig("lagom.broker.pubsub.client.consumer"))

  private final class ConsumerConfigImpl(conf: Config) extends ConsumerConfig {

    override val subscriptionName: String = conf.getString("subscription-name")
    override val ackDeadline: Int = conf.getInt("ack-deadline")
  }

}

