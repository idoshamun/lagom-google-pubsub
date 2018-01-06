package com.elegantmonkeys.lagom.scaladsl.broker.pubsub

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.elegantmonkeys.lagom.internal.scaladsl.broker.pubsub.PubsubTopicFactory
import com.lightbend.lagom.internal.scaladsl.api.broker.{TopicFactory, TopicFactoryProvider}
import com.lightbend.lagom.scaladsl.api.ServiceInfo

import scala.concurrent.ExecutionContext

trait LagomGooglePubsubClientComponents extends TopicFactoryProvider {
  def serviceInfo: ServiceInfo

  def actorSystem: ActorSystem

  def materializer: Materializer

  def executionContext: ExecutionContext

  lazy val topicFactory: TopicFactory = new PubsubTopicFactory(serviceInfo, actorSystem)(materializer, executionContext)

  override def optionalTopicFactory: Option[TopicFactory] = Some(topicFactory)
}
