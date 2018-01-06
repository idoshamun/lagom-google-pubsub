package com.lightbend.lagom.internal.javadsl.broker.pubsub

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.lightbend.lagom.internal.broker.pubsub.PubsubConfig
import com.lightbend.lagom.javadsl.api.Descriptor.TopicCall
import com.lightbend.lagom.javadsl.api.ServiceInfo
import com.lightbend.lagom.javadsl.api.broker.Topic.TopicId
import com.lightbend.lagom.javadsl.api.broker.{Subscriber, Topic}

import scala.concurrent.ExecutionContext

private[lagom] class JavadslPubsubTopic[Message](pubsubConfig: PubsubConfig,
                                                 topicCall: TopicCall[Message],
                                                 info: ServiceInfo,
                                                 system: ActorSystem)
                                                (implicit mat: Materializer,
                                                 ec: ExecutionContext) extends Topic[Message] {
  override def topicId: TopicId = topicCall.topicId

  override def subscribe: Subscriber[Message] = new JavadslPubsubSubscriber(pubsubConfig, topicCall,
    JavadslPubsubSubscriber.GroupId.default(info), info, system)
}

