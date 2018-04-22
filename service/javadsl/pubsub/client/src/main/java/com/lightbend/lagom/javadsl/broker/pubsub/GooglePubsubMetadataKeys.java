package com.lightbend.lagom.javadsl.broker.pubsub;

import com.lightbend.lagom.javadsl.api.broker.MetadataKey;

import java.time.Instant;
import java.util.Map;

/**
 * Metadata keys specific to the Google Pub/Sub broker implementation.
 */
public final class GooglePubsubMetadataKeys {
    private GooglePubsubMetadataKeys() {
    }

    public static final MetadataKey<String> ID = MetadataKey.named("pubsubId");

    public static final MetadataKey<Map<String, String>> ATTRIBUTES = MetadataKey.named("pubsubAttributes");

    public static final MetadataKey<Instant> TIMESTAMP = MetadataKey.named("pubsubTimestamp");
}
