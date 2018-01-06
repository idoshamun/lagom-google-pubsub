package com.elegantmonkeys.lagom.internal.scaladsl.broker.pubsub

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorSystem, SupervisorStrategy}
import akka.pattern.BackoffSupervisor
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.elegantmonkeys.lagom.internal.broker.pubsub.{ConsumerConfig, PubsubConfig, PubsubSubscriberActor}
import com.google.pubsub.v1.PubsubMessage
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.{ServiceInfo, ServiceLocator}
import com.lightbend.lagom.scaladsl.api.broker.Subscriber
import com.lightbend.lagom.scaladsl.api.deser.MessageSerializer.NegotiatedDeserializer
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * A Consumer for consuming messages from Google Pub/Sub.
  */
private[lagom] class ScaladslPubsubSubscriber[Message](pubsubConfig: PubsubConfig,
                                                       topicCall: TopicCall[Message],
                                                       groupId: Subscriber.GroupId,
                                                       info: ServiceInfo,
                                                       system: ActorSystem)
                                                      (implicit mat: Materializer, ec: ExecutionContext)
  extends Subscriber[Message] {

  private val log = LoggerFactory.getLogger(classOf[ScaladslPubsubSubscriber[_]])

  import ScaladslPubsubSubscriber._

  private lazy val consumerId = PubsubClientIdSequenceNumber.getAndIncrement

  private def consumerConfig = ConsumerConfig(system.settings.config)

  private def deserialize(message: PubsubMessage): Message = {
    val messageSerializer = topicCall.messageSerializer
    val protocol = messageSerializer.serializerForRequest.protocol
    val negotiatedDeserializer: NegotiatedDeserializer[Message, ByteString] =
      messageSerializer.deserializer(protocol)

    negotiatedDeserializer.deserialize(ByteString(message.getData.asReadOnlyByteBuffer()))
  }

  override def withGroupId(groupId: String): Subscriber[Message] = {
    val newGroupId = {
      if (groupId == null) {
        val defaultGroupId = GroupId.default(info)
        log.debug {
          "Passed a null groupId, but Kinesis requires clients to set one. " +
            s"Defaulting $this consumer groupId to $defaultGroupId."
        }
        defaultGroupId
      } else GroupId(groupId)
    }

    if (newGroupId.groupId == groupId) this
    else new ScaladslPubsubSubscriber(pubsubConfig, topicCall, newGroupId, info, system)
  }

  override def atMostOnceSource: Source[Message, _] = ???

  override def atLeastOnce(flow: Flow[Message, Done, _]): Future[Done] = {
    val streamCompleted = Promise[Done]
    val consumerProps = PubsubSubscriberActor.props(pubsubConfig, consumerConfig, topicCall.topicId.name,
      flow, streamCompleted, deserialize)


    val backoffConsumerProps = BackoffSupervisor.propsWithSupervisorStrategy(
      consumerProps,
      s"PubsubConsumerActor$consumerId-${topicCall.topicId.name}",
      consumerConfig.minBackoff,
      consumerConfig.maxBackoff,
      consumerConfig.randomBackoffFactor,
      SupervisorStrategy.stoppingStrategy)

    system.actorOf(backoffConsumerProps, s"PubsubBackoffConsumer$consumerId-${topicCall.topicId.name}")

    streamCompleted.future
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