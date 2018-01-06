package com.elegantmonkeys.lagom.internal.scaladsl.broker.pubsub

import akka.actor.ActorSystem
import akka.persistence.query.Offset
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.elegantmonkeys.lagom.internal.broker.pubsub.{Producer, PubsubConfig}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.lightbend.internal.broker.TaggedOffsetTopicProducer
import com.lightbend.lagom.internal.scaladsl.api.broker.TopicFactory
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.ServiceInfo
import com.lightbend.lagom.scaladsl.api.ServiceSupport.ScalaMethodTopic
import com.lightbend.lagom.scaladsl.server.LagomServer
import com.lightbend.lagom.spi.persistence.OffsetStore
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

class ScaladslRegisterTopicProducers(lagomServer: LagomServer, topicFactory: TopicFactory,
                                     info: ServiceInfo, actorSystem: ActorSystem, offsetStore: OffsetStore)
                                    (implicit ec: ExecutionContext, mat: Materializer) {

  private val log = LoggerFactory.getLogger(classOf[ScaladslRegisterTopicProducers])
  private val pubsubConfig = PubsubConfig(actorSystem.settings.config)

  private def serialize[Message](topicCall: TopicCall[Message])(message: Message): PubsubMessage = {
    val data = ByteString.copyFrom(
      topicCall.messageSerializer.serializerForRequest.serialize(message).asByteBuffer
    )
    PubsubMessage.newBuilder.setData(data).build()
  }

  // Goes through the services' descriptors and publishes the streams registered in
  // each of the service's topic method implementation.
  for {
    service <- lagomServer.serviceBindings
    tc <- service.descriptor.topics
    topicCall = tc.asInstanceOf[TopicCall[Any]]
  } {
    topicCall.topicHolder match {
      case holder: ScalaMethodTopic[_] =>
        val topicProducer = holder.method.invoke(service.service)
        val topicId = topicCall.topicId

        topicFactory.create(topicCall) match {
          case topicImpl: ScaladslPubsubTopic[Any] =>

            topicProducer match {
              case tagged: TaggedOffsetTopicProducer[_, _] =>

                val tags = tagged.tags

                val eventStreamFactory: (String, Offset) => Source[(Any, Offset), _] = { (tag, offset) =>
                  tags.find(_.tag == tag) match {
                    case Some(aggregateTag) => tagged.readSideStream(aggregateTag, offset)
                    case None => throw new RuntimeException("Unknown tag: " + tag)
                  }
                }

                Producer.startTaggedOffsetProducer(
                  actorSystem,
                  tags.map(_.tag),
                  pubsubConfig,
                  topicId.name,
                  eventStreamFactory,
                  serialize(topicCall),
                  offsetStore)
              case other => log.warn {
                s"Unknown topic producer ${other.getClass.getName}. " +
                  s"This will likely result in no events published to topic ${topicId.name} " +
                  s"by service ${info.serviceName}."
              }
            }

          case otherTopicImpl => log.warn {
            s"Expected Topic type ${classOf[ScaladslPubsubTopic[_]].getName}, " +
              s"but found incompatible type ${otherTopicImpl.getClass.getName}." +
              s"This will likely result in no events published to topic ${topicId.name} by service ${info.serviceName}."
          }
        }

      case other =>
        log.error {
          s"Cannot plug publisher source for topic ${topicCall.topicId}. " +
            s"Reason was that it was expected a topicHolder of type ${classOf[ScalaMethodTopic[_]]}, " +
            s"but ${other.getClass} was found instead."
        }
    }
  }

}

