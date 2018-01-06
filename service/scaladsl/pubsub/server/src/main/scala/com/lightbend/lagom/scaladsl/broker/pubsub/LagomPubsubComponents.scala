package com.lightbend.lagom.scaladsl.broker.pubsub

import com.lightbend.lagom.internal.scaladsl.broker.pubsub.ScaladslRegisterTopicProducers
import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.server.LagomServer
import com.lightbend.lagom.spi.persistence.OffsetStore

/**
  * Components for including Google Pub/Sub into a Lagom application.
  *
  * Extending this trait will automatically start all topic producers.
  */
trait LagomPubsubComponents extends LagomGooglePubsubClientComponents {
  def lagomServer: LagomServer

  def offsetStore: OffsetStore

  def serviceLocator: ServiceLocator

  override def topicPublisherName: Option[String] = super.topicPublisherName match {
    case Some(other) =>
      sys.error(s"Cannot provide the Google Pub/Sub topic factory as the default " +
        s"topic publisher since a default topic publisher " +
        s"has already been mixed into this cake: $other")
    case None => Some("pubsub")
  }

  // Eagerly start topic producers
  new ScaladslRegisterTopicProducers(lagomServer, topicFactory, serviceInfo, actorSystem,
    offsetStore)(executionContext, materializer)
}
