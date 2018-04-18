package com.lightbend.lagom.internal.scaladsl.broker.pubsub

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorSystem, SupervisorStrategy}
import akka.pattern.BackoffSupervisor
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.lightbend.lagom.internal.broker.pubsub.{ConsumerConfig, PubsubConfig, PubsubSubscriberActor}
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.ServiceInfo
import com.lightbend.lagom.scaladsl.api.broker.{Message, Subscriber}
import com.lightbend.lagom.scaladsl.api.deser.MessageSerializer.NegotiatedDeserializer
import com.lightbend.lagom.scaladsl.broker.pubsub.GooglePubsubMetadataKeys
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.collection.JavaConverters._

/**
  * A Consumer for consuming messages from Google Pub/Sub.
  */
private[lagom] class ScaladslPubsubSubscriber[Payload, SubscriberPayload]
(pubsubConfig: PubsubConfig, topicCall: TopicCall[Payload], groupId: Subscriber.GroupId,
 info: ServiceInfo, system: ActorSystem, transform: (PubsubMessage, Payload) => SubscriberPayload)
(implicit mat: Materializer, ec: ExecutionContext) extends Subscriber[SubscriberPayload] {

  private val log = LoggerFactory.getLogger(classOf[ScaladslPubsubSubscriber[_, _]])

  import ScaladslPubsubSubscriber._

  private lazy val consumerId = PubsubClientIdSequenceNumber.getAndIncrement

  private val consumerConfig = ConsumerConfig(system.settings.config)
  private val subscriptionName = PubsubSubscriberActor.subscriptionName(groupId.groupId, topicCall.topicId.name)

  private def deserialize(message: PubsubMessage): SubscriberPayload = {
    val messageSerializer = topicCall.messageSerializer
    val protocol = messageSerializer.serializerForRequest.protocol
    val negotiatedDeserializer: NegotiatedDeserializer[Payload, ByteString] =
      messageSerializer.deserializer(protocol)

    val payload = negotiatedDeserializer.deserialize(ByteString(message.getData.asReadOnlyByteBuffer()))
    message.getPublishTime.getNanos
    transform(message, payload)
  }

  override def withGroupId(groupIdName: String): Subscriber[SubscriberPayload] = {
    val newGroupId = {
      if (groupIdName == null) {
        GroupId.default(info)
      } else GroupId(groupIdName)
    }

    if (newGroupId == groupId) this
    else new ScaladslPubsubSubscriber(pubsubConfig, topicCall, newGroupId, info, system, transform)
  }

  override def withMetadata: Subscriber[Message[SubscriberPayload]] =
    new ScaladslPubsubSubscriber[Payload, Message[SubscriberPayload]](pubsubConfig, topicCall,
      groupId, info, system, wrapPayload)

  override def atMostOnceSource: Source[SubscriberPayload, _] = ???

  override def atLeastOnce(flow: Flow[SubscriberPayload, Done, _]): Future[Done] = {
    val streamCompleted = Promise[Done]
    val consumerProps = PubsubSubscriberActor.props(pubsubConfig, consumerConfig, subscriptionName,
      topicCall.topicId.name, flow, streamCompleted, deserialize)


    val backoffConsumerProps = BackoffSupervisor.propsWithSupervisorStrategy(
      consumerProps,
      s"PubsubConsumerActor$consumerId-${topicCall.topicId.name}",
      consumerConfig.minBackoff,
      consumerConfig.maxBackoff,
      consumerConfig.randomBackoffFactor,
      SupervisorStrategy.stoppingStrategy
    )

    system.actorOf(backoffConsumerProps, s"PubsubBackoffConsumer$consumerId-${topicCall.topicId.name}")

    streamCompleted.future
  }

  private def wrapPayload(message: PubsubMessage, payload: Payload): Message[SubscriberPayload] = {
    Message(transform(message, payload)) +
      (GooglePubsubMetadataKeys.Id -> message.getMessageId) +
      (GooglePubsubMetadataKeys.Attributes -> message.getAttributesMap.asScala.toMap) +
      (GooglePubsubMetadataKeys.Timestamp ->
        Instant.ofEpochSecond(message.getPublishTime.getSeconds, message.getPublishTime.getNanos))
  }
}

private[lagom] object ScaladslPubsubSubscriber {
  private val PubsubClientIdSequenceNumber = new AtomicInteger(1)

  case class GroupId(groupId: String) extends Subscriber.GroupId {
    if (GroupId.isInvalidGroupId(groupId))
      throw new IllegalArgumentException(s"Failed to create group because [groupId=$groupId] " +
        s"contains invalid character(s).")
  }

  case object GroupId {
    private val InvalidGroupIdChars =
      Set('/', '\\', ',', '\u0000', ':', '"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=')

    private def isInvalidGroupId(groupId: String): Boolean = groupId.exists(InvalidGroupIdChars.apply)

    def default(info: ServiceInfo): GroupId = GroupId(info.serviceName)
  }

}