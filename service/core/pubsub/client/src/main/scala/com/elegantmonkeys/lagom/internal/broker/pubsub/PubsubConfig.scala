package com.elegantmonkeys.lagom.internal.broker.pubsub

import com.typesafe.config.Config

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

sealed trait ClientConfig {

}

object ClientConfig {

  private[pubsub] class ClientConfigImpl(conf: Config) extends ClientConfig {

  }

}

sealed trait ProducerConfig extends ClientConfig {
  def role: Option[String]
}

object ProducerConfig {
  def apply(conf: Config): ProducerConfig =
    new ProducerConfigImpl(conf.getConfig("lagom.broker.pubsub.client.producer"))

  private class ProducerConfigImpl(conf: Config)
    extends ClientConfig.ClientConfigImpl(conf) with ProducerConfig {

    val role: Option[String] = conf.getString("role") match {
      case "" => None
      case other => Some(other)
    }
  }

}

sealed trait ConsumerConfig extends ClientConfig {
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

  private final class ConsumerConfigImpl(conf: Config)
    extends ClientConfig.ClientConfigImpl(conf) with ConsumerConfig {

    override val subscriptionName: String = conf.getString("subscription-name")
    override val ackDeadline: Int = conf.getInt("ack-deadline")
  }

}

