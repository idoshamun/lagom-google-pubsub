package com.elegantmonkeys.lagom.internal.broker.pubsub

import java.io.FileInputStream

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.pattern.pipe
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.elegantmonkeys.lagom.internal.broker.pubsub.Protocol._
import com.elegantmonkeys.lagom.internal.broker.pubsub.PubsubSubscriberActor._
import com.google.api.gax.core.{CredentialsProvider, FixedCredentialsProvider}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, SubscriptionAdminSettings}
import com.google.pubsub.v1.{PubsubMessage, PushConfig, SubscriptionName, TopicName}

import scala.concurrent.{ExecutionContext, Future, Promise}

private[lagom] class PubsubSubscriberActor[Message](pubsubConfig: PubsubConfig,
                                                    consumerConfig: ConsumerConfig,
                                                    topicId: String,
                                                    flow: Flow[Message, Done, _],
                                                    streamCompleted: Promise[Done],
                                                    transform: PubsubMessage => Message)
                                                   (implicit mat: Materializer, ec: ExecutionContext)
  extends Actor with ActorLogging {

  /** Switch used to terminate the on-going Pub/Sub publishing stream when this actor fails. */
  private var shutdown: Option[KillSwitch] = None

  override def preStart(): Unit = {
    val topic: TopicName = TopicName.of(pubsubConfig.projectId, topicId)
    val subscription: SubscriptionName = SubscriptionName.of(pubsubConfig.projectId, consumerConfig.subscriptionName)
    val credentials: CredentialsProvider =
      FixedCredentialsProvider.create(
        ServiceAccountCredentials.fromStream(new FileInputStream(pubsubConfig.serviceAccountPath)))

    createSubscription(consumerConfig, topic, subscription, credentials).map(_ => SubscriptionCreated) pipeTo self
    context.become(creatingSubscription(topic, subscription, credentials))
  }

  override def postStop(): Unit = shutdown.foreach(_.shutdown())

  override def receive: Receive = PartialFunction.empty

  private def creatingSubscription(topic: TopicName, subscription: SubscriptionName,
                                   credentials: CredentialsProvider): Receive = {
    case SubscriptionCreated =>
      log.debug("Subscription [{}] created", subscription.getSubscription)
      run(subscription, credentials)
  }

  private def run(subscription: SubscriptionName, credentials: CredentialsProvider): Unit = {
    val source: Source[PubsubMessage, NotUsed] = Source.fromGraph(new PubsubSource(subscription, credentials))

    val (killSwitch, stream) =
      source
        .map[Message](transform)
        .via(flow)
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
}

object PubsubSubscriberActor {
  def createSubscription(consumerConfig: ConsumerConfig, topic: TopicName,
                         subscription: SubscriptionName, credentials: CredentialsProvider)
                        (implicit ec: ExecutionContext): Future[Unit] = Future {
    val settings: SubscriptionAdminSettings = SubscriptionAdminSettings
      .newBuilder()
      .setCredentialsProvider(credentials)
      .build()

    val client: SubscriptionAdminClient = SubscriptionAdminClient.create(settings)
    client.createSubscription(subscription, topic, PushConfig.getDefaultInstance, consumerConfig.ackDeadline)
  }

  def props[Message](pubsubConfig: PubsubConfig, consumerConfig: ConsumerConfig, topicId: String,
                     flow: Flow[Message, Done, _], streamCompleted: Promise[Done], transform: PubsubMessage => Message)
                    (implicit mat: Materializer, ec: ExecutionContext) =
    Props(new PubsubSubscriberActor[Message](pubsubConfig, consumerConfig, topicId, flow, streamCompleted, transform))
}

private object Protocol {

  case object SubscriptionCreated

}