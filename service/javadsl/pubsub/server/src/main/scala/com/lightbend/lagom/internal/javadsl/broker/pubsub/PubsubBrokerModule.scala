package com.lightbend.lagom.internal.javadsl.broker.pubsub

import com.google.inject.AbstractModule

class PubsubBrokerModule extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[JavadslRegisterTopicProducers]).asEagerSingleton()
  }
}
