package com.lightbend.lagom.internal.broker.pubsub

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.google.api.core.ApiService
import com.google.api.core.ApiService.Listener
import com.google.api.gax.core.CredentialsProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1.{PubsubMessage, SubscriptionName}

private[pubsub] final class PubsubSource(subscription: SubscriptionName, credentials: CredentialsProvider)
  extends GraphStage[SourceShape[PubsubMessage]] {

  val out: Outlet[PubsubMessage] = Outlet("PubsubSource.out")
  override val shape: SourceShape[PubsubMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private val receiver = new MessageReceiver {
      override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
        push(out, message)
        consumer.ack()
      }
    }

    private val subscriber =
      Subscriber
        .newBuilder(subscription, receiver)
        .setCredentialsProvider(credentials)
        .build()

    subscriber.addListener(new Listener {
      override def failed(from: ApiService.State, failure: Throwable): Unit = {
        fail(out, failure)
      }
    }, MoreExecutors.directExecutor())

    subscriber.startAsync()

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {}

      override def onDownstreamFinish(): Unit = {
        subscriber.stopAsync()
        super.onDownstreamFinish()
      }
    })
  }
}
