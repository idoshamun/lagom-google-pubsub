package com.lightbend.lagom.internal.scaladsl.broker.pubsub

import org.scalatest.{Matchers, FlatSpec}

class ScaladslPubsubSubscriberSpec extends FlatSpec with Matchers {

  behavior of "ScaladslPubsubSubscriber"

  it should "create a new subscriber with updated groupId" in {
    val subscriber = new ScaladslPubsubSubscriber(null, null, ScaladslPubsubSubscriber.GroupId("old"), null, null, null)(null, null)
    subscriber.withGroupId("newGID") should not be subscriber
  }
}
