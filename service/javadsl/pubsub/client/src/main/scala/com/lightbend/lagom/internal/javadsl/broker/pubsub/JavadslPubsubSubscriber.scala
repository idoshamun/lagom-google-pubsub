package com.lightbend.lagom.internal.javadsl.broker.pubsub

import java.time.Instant
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorSystem, SupervisorStrategy}
import akka.pattern.BackoffSupervisor
import akka.stream.Materializer
import akka.stream.javadsl.{Flow, Source}
import akka.util.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.lightbend.lagom.internal.broker.pubsub.{ConsumerConfig, PubsubConfig, PubsubSubscriberActor}
import com.lightbend.lagom.javadsl.api.Descriptor.TopicCall
import com.lightbend.lagom.javadsl.api.ServiceInfo
import com.lightbend.lagom.javadsl.api.broker.Subscriber
import com.lightbend.lagom.javadsl.api.deser.MessageSerializer.NegotiatedDeserializer
import com.lightbend.lagom.javadsl.api.broker.Message
import com.lightbend.lagom.javadsl.broker.pubsub.GooglePubsubMetadataKeys
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Promise}
import scala.compat.java8.FutureConverters._

private[lagom] class JavadslPubsubSubscriber[Payload, SubscriberPayload]
(pubsubConfig: PubsubConfig, topicCall: TopicCall[Payload], groupId: Subscriber.GroupId,
 info: ServiceInfo, system: ActorSystem, transform: (PubsubMessage, Payload) => SubscriberPayload)
(implicit mat: Materializer, ec: ExecutionContext)
  extends Subscriber[SubscriberPayload] {

  private val log = LoggerFactory.getLogger(classOf[JavadslPubsubSubscriber[_, _]])

  import JavadslPubsubSubscriber._

  private lazy val consumerId = PubsubClientIdSequenceNumber.getAndIncrement

  private def consumerConfig = ConsumerConfig(system.settings.config)

  private lazy val subscriptionName = PubsubSubscriberActor.subscriptionName(groupId.groupId, topicCall.topicId.value)

  private def deserialize(message: PubsubMessage): SubscriberPayload = {
    val messageSerializer = topicCall.messageSerializer
    val protocol = messageSerializer.serializerForRequest.protocol
    val negotiatedDeserializer: NegotiatedDeserializer[Payload, ByteString] =
      messageSerializer.deserializer(protocol)

    val payload = negotiatedDeserializer.deserialize(ByteString(message.getData.asReadOnlyByteBuffer()))
    transform(message, payload)
  }

  override def withGroupId(groupId: String): Subscriber[SubscriberPayload] = {
    val newGroupId = {
      if (groupId == null) {
        GroupId.default(info)
      } else GroupId(groupId)
    }

    if (newGroupId == groupId) this
    else new JavadslPubsubSubscriber(pubsubConfig, topicCall, newGroupId, info, system, transform)
  }

  override def withMetadata: Subscriber[Message[SubscriberPayload]] =
    new JavadslPubsubSubscriber[Payload, Message[SubscriberPayload]](pubsubConfig, topicCall,
      groupId, info, system, wrapPayload)

  override def atMostOnceSource: Source[SubscriberPayload, _] = ???

  override def atLeastOnce(flow: Flow[SubscriberPayload, Done, _]): CompletionStage[Done] = {
    val streamCompleted = Promise[Done]
    val consumerProps = PubsubSubscriberActor.props(pubsubConfig, consumerConfig, subscriptionName,
      topicCall.topicId.value, flow.asScala, streamCompleted, deserialize)


    val backoffConsumerProps = BackoffSupervisor.propsWithSupervisorStrategy(
      consumerProps,
      s"PubsubConsumerActor$consumerId-${topicCall.topicId.value}",
      consumerConfig.minBackoff,
      consumerConfig.maxBackoff,
      consumerConfig.randomBackoffFactor,
      SupervisorStrategy.stoppingStrategy)

    system.actorOf(backoffConsumerProps, s"PubsubBackoffConsumer$consumerId-${topicCall.topicId.value}")

    streamCompleted.future.toJava
  }

  private def wrapPayload(message: PubsubMessage, payload: Payload): Message[SubscriberPayload] = {
    Message.create(transform(message, payload))
      .add(GooglePubsubMetadataKeys.ID, message.getMessageId)
      .add(GooglePubsubMetadataKeys.ATTRIBUTES, message.getAttributesMap)
      .add(GooglePubsubMetadataKeys.TIMESTAMP, Instant.ofEpochSecond(message.getPublishTime.getSeconds, message.getPublishTime.getNanos))
  }
}

private[lagom] object JavadslPubsubSubscriber {
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