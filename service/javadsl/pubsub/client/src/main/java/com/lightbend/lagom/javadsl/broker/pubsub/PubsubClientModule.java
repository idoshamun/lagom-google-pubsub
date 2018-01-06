package com.lightbend.lagom.javadsl.broker.pubsub;

import com.google.inject.AbstractModule;
import com.lightbend.lagom.internal.javadsl.api.broker.TopicFactory;

public class PubsubClientModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TopicFactory.class).to(PubsubTopicFactory.class);
    }
}
