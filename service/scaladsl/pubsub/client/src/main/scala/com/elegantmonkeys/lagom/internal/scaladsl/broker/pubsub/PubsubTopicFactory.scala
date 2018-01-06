package com.elegantmonkeys.lagom.internal.scaladsl.broker.pubsub

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.elegantmonkeys.lagom.internal.broker.pubsub.PubsubConfig
import com.lightbend.lagom.internal.scaladsl.api.broker.TopicFactory
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.ServiceInfo
import com.lightbend.lagom.scaladsl.api.broker.Topic

import scala.concurrent.ExecutionContext

/**
  * Factory for creating topics instances.
  */
private[lagom] class PubsubTopicFactory(serviceInfo: ServiceInfo, system: ActorSystem)
                                       (implicit materializer: Materializer,
                                        executionContext: ExecutionContext) extends TopicFactory {
  private val config = PubsubConfig(system.settings.config)

  def create[Message](topicCall: TopicCall[Message]): Topic[Message] =
    new ScaladslPubsubTopic(config, topicCall, serviceInfo, system)
}
