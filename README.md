# Lagom Google Pub/Sub provider

An Implementation of Lagom Message Broker API for Google Pub/Sub.

This project is currently still an early experiment and is not suitable for production use.

This project is derived from:
* Kafka implementation embedded in Lagom by Lightbend
* [Kinesis implementation by StreetContxt](https://github.com/StreetContxt/lagom-kinesis)

### Installation

#### Producer

Add the following to your `build.sbt`

`libraryDependencies += "com.elegantmonkeys" %% "lagom-scaladsl-google-pubsub-broker" % "1.0.0-RC1"

#### Subscriber

Add the following to your `build.sbt`

`libraryDependencies += "com.elegantmonkeys" %% "lagom-scaladsl-google-pubsub-client" % "1.0.0-RC1" 