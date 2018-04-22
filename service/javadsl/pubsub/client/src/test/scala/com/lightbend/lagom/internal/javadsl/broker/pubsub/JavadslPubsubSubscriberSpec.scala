package com.lightbend.lagom.internal.javadsl.broker.pubsub

import org.scalatest.{Matchers, FlatSpec}

class JavadslPubsubSubscriberSpec extends FlatSpec with Matchers {

  behavior of "JavadslPubsubSubscriber"

  it should "create a new subscriber with updated groupId" in {
    val subscriber = new JavadslPubsubSubscriber(null, null, JavadslPubsubSubscriber.GroupId("old"), null, null, null)(null, null)
    subscriber.withGroupId("newGID") should not be subscriber
  }
}
