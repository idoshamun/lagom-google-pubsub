package com.lightbend.lagom.internal.broker.pubsub

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.pubsub.v1.{ProjectSubscriptionName, PullRequest, ReceivedMessage}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

private[pubsub] final class PubsubSource(subscription: ProjectSubscriptionName, subscriber: GrpcSubscriberStub,
                                         pullingInterval: Int)(implicit ec: ExecutionContext)
  extends GraphStage[SourceShape[ReceivedMessage]] {

  private val log = LoggerFactory.getLogger(classOf[PubsubSource])

  val out: Outlet[ReceivedMessage] = Outlet("PubsubSource.out")
  override val shape: SourceShape[ReceivedMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    import PubsubSource._

    private var state: State = Pending

    private val callback = getAsyncCallback[ReceivedMessage] { message =>
      state = Pending
      push(out, message)
    }

    private def pull(): Future[Unit] = {
      Future {
        // Setting return immediately to false sometimes skip messages
        val pullRequest = PullRequest.newBuilder().setMaxMessages(1).setReturnImmediately(true)
          .setSubscription(subscription.toString).build()
        val pullResponse = subscriber.pullCallable().call(pullRequest)
        pullResponse.getReceivedMessagesList.asScala
      }.flatMap { list =>
        if (list.isEmpty && !isClosed(out)) {
          Thread.sleep(pullingInterval)
          pull()
        } else {
          if (list.nonEmpty) log.debug("message received from pub/sub")
          else log.debug("stopping pulling messages from pub/sub")
          Future.successful(list.foreach(callback.invoke))
        }
      }
    }

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        state match {
          case Pending =>
            state = Pulling
            pull()
          case Pulling =>
        }
      }
    })
  }
}

private object PubsubSource {

  private sealed trait State

  private case object Pending extends State

  private case object Pulling extends State

}
