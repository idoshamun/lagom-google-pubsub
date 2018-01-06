package com.elegantmonkeys.lagom.internal.scaladsl.broker.pubsub

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.elegantmonkeys.lagom.internal.broker.pubsub.PubsubConfig
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.ServiceInfo
import com.lightbend.lagom.scaladsl.api.broker.Topic.TopicId
import com.lightbend.lagom.scaladsl.api.broker.{Subscriber, Topic}

import scala.concurrent.ExecutionContext

private[lagom] class ScaladslPubsubTopic[Message](pubsubConfig: PubsubConfig,
                                                  topicCall: TopicCall[Message],
                                                  info: ServiceInfo,
                                                  system: ActorSystem)
                                                 (implicit mat: Materializer,
                                                  ec: ExecutionContext) extends Topic[Message] {
  override def topicId: TopicId = topicCall.topicId

  override def subscribe: Subscriber[Message] = new ScaladslPubsubSubscriber(pubsubConfig, topicCall,
    ScaladslPubsubSubscriber.GroupId.default(info), info, system)
}
