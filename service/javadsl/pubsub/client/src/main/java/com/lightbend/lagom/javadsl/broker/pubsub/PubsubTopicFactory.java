package com.lightbend.lagom.javadsl.broker.pubsub;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import com.lightbend.lagom.internal.broker.pubsub.PubsubConfig;
import com.lightbend.lagom.internal.broker.pubsub.PubsubConfig$;
import com.lightbend.lagom.internal.javadsl.api.broker.TopicFactory;
import com.lightbend.lagom.internal.javadsl.broker.pubsub.JavadslPubsubTopic;
import com.lightbend.lagom.javadsl.api.Descriptor;
import com.lightbend.lagom.javadsl.api.ServiceInfo;
import com.lightbend.lagom.javadsl.api.broker.Topic;
import scala.concurrent.ExecutionContext;

import javax.inject.Inject;

/**
 * Factory for creating topics instances.
 */
public class PubsubTopicFactory implements TopicFactory {
    private final ServiceInfo serviceInfo;
    private final ActorSystem system;
    private final Materializer materializer;
    private final ExecutionContext executionContext;
    private final PubsubConfig config;

    @Inject
    public PubsubTopicFactory(ServiceInfo serviceInfo, ActorSystem system, Materializer materializer,
                              ExecutionContext executionContext) {
        this.serviceInfo = serviceInfo;
        this.system = system;
        this.materializer = materializer;
        this.executionContext = executionContext;
        this.config = PubsubConfig$.MODULE$.apply(system.settings().config());
    }

    @Override
    public <Message> Topic<Message> create(Descriptor.TopicCall<Message> topicCall) {
        return new JavadslPubsubTopic<>(config, topicCall, serviceInfo, system, materializer, executionContext);
    }
}
