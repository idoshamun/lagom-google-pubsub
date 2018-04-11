package com.lightbend.lagom.scaladsl.broker.pubsub

import java.time.Instant

import com.lightbend.lagom.scaladsl.api.broker.MetadataKey

/**
  * Metadata keys specific to the Google Pub/Sub broker implementation.
  */
object GooglePubsubMetadataKeys {
  val Id: MetadataKey[String] = MetadataKey("pubsubId")
  val Attributes: MetadataKey[Map[String, String]] = MetadataKey("pubsubAttributes")
  val Timestamp: MetadataKey[Instant] = MetadataKey("pubsubTimestamp")
}
