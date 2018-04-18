package com.lightbend.lagom.internal.broker.pubsub

import java.io.FileInputStream

import akka.Done
import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.pattern.pipe
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Unzip, Zip}
import PubsubSubscriberActor._
import com.google.api.gax.core.{FixedCredentialsProvider, NoCredentialsProvider}
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{AlreadyExistsException, ClientSettings, FixedTransportChannelProvider}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1._
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1._
import io.grpc.ManagedChannelBuilder

import scala.concurrent.{ExecutionContext, Future, Promise}

private[lagom] class PubsubSubscriberActor[Message](pubsubConfig: PubsubConfig,
                                                    consumerConfig: ConsumerConfig,
                                                    subscriptionName: String,
                                                    topicId: String,
                                                    flow: Flow[Message, Done, _],
                                                    streamCompleted: Promise[Done],
                                                    transform: PubsubMessage => Message)
                                                   (implicit mat: Materializer, ec: ExecutionContext)
  extends Actor with ActorLogging {

  /** Switch used to terminate the on-going Pub/Sub publishing stream when this actor fails. */
  private var shutdown: Option[KillSwitch] = None

  private val subscriberStubSettings = {
    val builder = SubscriberStubSettings.newBuilder()
    pubsubConfig.emulatorHost
      .map { host =>
        val channel = ManagedChannelBuilder.forTarget(host).usePlaintext(true).build()
        val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
        builder
          .setTransportChannelProvider(channelProvider)
          .setCredentialsProvider(NoCredentialsProvider.create).build
      }
      .getOrElse {
        pubsubConfig.serviceAccountPath
          .map { path =>
            builder.setCredentialsProvider(
              FixedCredentialsProvider.create(
                ServiceAccountCredentials.fromStream(
                  new FileInputStream(path)))).build()
          }
          .getOrElse {
            builder.build()
          }
      }
  }
  private val subscriber = GrpcSubscriberStub.create(subscriberStubSettings)

  override def preStart(): Unit = {
    val topic: ProjectTopicName = ProjectTopicName.of(pubsubConfig.projectId, topicId)
    createTopic(topic, pubsubConfig)
    log.debug("Topic [{}] found/created", topic.getTopic)

    val subscription = ProjectSubscriptionName.of(pubsubConfig.projectId, subscriptionName)
    createSubscription(consumerConfig, topic, subscription, pubsubConfig)
    log.debug("Subscription [{}] created", subscription.getSubscription)

    run(subscription)
  }

  override def postStop(): Unit = {
    shutdown.foreach(_.shutdown())
  }

  override def receive: Receive = PartialFunction.empty

  private def run(subscription: ProjectSubscriptionName): Unit = {
    val (killSwitch, stream) =
      atLeastOnce(subscription)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

    shutdown = Some(killSwitch)
    stream pipeTo self
    context.become(running)
  }

  private def running: Receive = {
    case Status.Failure(e) =>
      log.error("Topic subscription interrupted due to failure", e)
      throw e

    case Done =>
      log.info("Pub/Sub subscriber stream for topic {} was completed.", topicId)
      streamCompleted.success(Done)
      context.stop(self)
  }

  private def atLeastOnce(subscription: ProjectSubscriptionName): Source[Done, _] = {
    val source = Source.fromGraph(new PubsubSource(subscription, subscriber, consumerConfig.pullingInterval))
      .map { message => (message.getAckId, transform(message.getMessage)) }

    val committOffsetFlow =
      Flow.fromGraph(GraphDSL.create(flow) { implicit builder =>
        flow =>
          import GraphDSL.Implicits._

          val unzip = builder.add(Unzip[String, Message])
          val zip = builder.add(Zip[String, Done])
          val committer = {
            val commitFlow = Flow[(String, Done)]
              .groupedWithin(consumerConfig.batchingSize, consumerConfig.batchingInterval)
              .map { group =>
                group.foldLeft(AcknowledgeRequest.newBuilder().setSubscription(subscription.toString)) { (batch, elem) =>
                  batch.addAckIds(elem._1)
                }.build()
              }
              .mapAsync(parallelism = 3) { ackRequest =>
                Future {
                  subscriber.acknowledgeCallable().call(ackRequest)
                  Done
                }
              }
            builder.add(commitFlow)
          }

          // To allow the user flow to do its own batching, the offset side of the flow needs to effectively buffer
          // infinitely to give full control of backpressure to the user side of the flow.
          val offsetBuffer = Flow[String].buffer(consumerConfig.offsetBuffer, OverflowStrategy.backpressure)

          unzip.out0 ~> offsetBuffer ~> zip.in0
          unzip.out1 ~> flow ~> zip.in1
          zip.out ~> committer.in

          FlowShape(unzip.in, committer.out)
      })

    source.via(committOffsetFlow)
  }
}

object PubsubSubscriberActor {
  def buildSettings[Settings <: ClientSettings[Settings], Builder <: ClientSettings.Builder[Settings, Builder]]
  (builder: Builder, pubsubConfig: PubsubConfig): Settings = {
    pubsubConfig.emulatorHost
      .map { host =>
        val channel = ManagedChannelBuilder.forTarget(host).usePlaintext(true).build()
        val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
        builder
          .setTransportChannelProvider(channelProvider)
          .setCredentialsProvider(NoCredentialsProvider.create).build
      }
      .getOrElse {
        pubsubConfig.serviceAccountPath
          .map { path =>
            builder.setCredentialsProvider(
              FixedCredentialsProvider.create(
                ServiceAccountCredentials.fromStream(
                  new FileInputStream(path)))).build()
          }
          .getOrElse {
            builder.build()
          }
      }
  }

  def createTopic(topic: ProjectTopicName, pubsubConfig: PubsubConfig): Unit = {
    val settings: TopicAdminSettings = buildSettings[TopicAdminSettings, TopicAdminSettings.Builder](TopicAdminSettings.newBuilder(), pubsubConfig)
    val client: TopicAdminClient = TopicAdminClient.create(settings)

    try {
      client.createTopic(topic)
    } catch {
      case _: AlreadyExistsException =>
    }
  }

  def createSubscription(consumerConfig: ConsumerConfig, topic: ProjectTopicName,
                         subscription: ProjectSubscriptionName, pubsubConfig: PubsubConfig): Unit = {
    val settings: SubscriptionAdminSettings = buildSettings[SubscriptionAdminSettings, SubscriptionAdminSettings.Builder](SubscriptionAdminSettings.newBuilder(), pubsubConfig)
    val client: SubscriptionAdminClient = SubscriptionAdminClient.create(settings)

    try {
      client.createSubscription(subscription, topic, PushConfig.getDefaultInstance, consumerConfig.ackDeadline)
    } catch {
      case _: AlreadyExistsException =>
    }
  }

  def deleteTopic(topic: ProjectTopicName, pubsubConfig: PubsubConfig): Unit = {
    val settings: TopicAdminSettings = buildSettings[TopicAdminSettings, TopicAdminSettings.Builder](TopicAdminSettings.newBuilder(), pubsubConfig)
    val client: TopicAdminClient = TopicAdminClient.create(settings)

    client.deleteTopic(topic)
  }

  def deleteSubscription(subscription: ProjectSubscriptionName, pubsubConfig: PubsubConfig): Unit = {
    val settings: SubscriptionAdminSettings = buildSettings[SubscriptionAdminSettings, SubscriptionAdminSettings.Builder](SubscriptionAdminSettings.newBuilder(), pubsubConfig)
    val client: SubscriptionAdminClient = SubscriptionAdminClient.create(settings)

    client.deleteSubscription(subscription)
  }

  def subscriptionName(groupId: String, topicId: String): String = s"$groupId-$topicId"

  def props[Message](pubsubConfig: PubsubConfig, consumerConfig: ConsumerConfig, subscriptionName: String, topicId: String,
                     flow: Flow[Message, Done, _], streamCompleted: Promise[Done], transform: PubsubMessage => Message)
                    (implicit mat: Materializer, ec: ExecutionContext) =
    Props(new PubsubSubscriberActor[Message](pubsubConfig, consumerConfig, subscriptionName, topicId, flow, streamCompleted, transform))
}

private object Protocol {

  case object TopicCreated

  case object SubscriptionCreated

}