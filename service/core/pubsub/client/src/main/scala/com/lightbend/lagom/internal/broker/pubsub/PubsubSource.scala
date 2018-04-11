package com.lightbend.lagom.internal.broker.pubsub

import java.io.FileInputStream

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.google.api.core.ApiService
import com.google.api.core.ApiService.Listener
import com.google.api.gax.core.{FixedCredentialsProvider, NoCredentialsProvider}
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import io.grpc.ManagedChannelBuilder

private[pubsub] final class PubsubSource(subscription: ProjectSubscriptionName, pubsubConfig: PubsubConfig)
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

    private val subscriber = {
      val builder = Subscriber.newBuilder(subscription, receiver)
      pubsubConfig.emulatorHost
        .map { host =>
          val channel = ManagedChannelBuilder.forTarget(host).usePlaintext(true).build()
          val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
          builder
            .setChannelProvider(channelProvider)
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
