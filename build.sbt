organization in ThisBuild := "com.elegantmonkeys"
scalaVersion in ThisBuild := "2.11.8"

val lagomVersion = "1.3.0"

val slf4j = "org.slf4j" % "log4j-over-slf4j" % "1.7.25"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4"
val lagomApi = "com.lightbend.lagom" %% "lagom-api" % lagomVersion
val lagomPersistenceCore = "com.lightbend.lagom" %% "lagom-persistence-core" % lagomVersion
val pubsubSdk = "com.google.cloud" % "google-cloud-pubsub" % "0.32.0-beta"

val pubsubProjects = Seq[Project]()

lazy val root = (project in file("."))
  .settings(name := "lagom-google-pubsub")
  .aggregate(pubsubProjects.map(Project.projectToRef): _*)

lazy val `client` = (project in file("service/core/pubsub/client"))
  .settings(name := "lagom-google-pubsub-client")
  .settings(
    libraryDependencies ++= Seq(
      slf4j,
      lagomApi,
      scalaTest % Test,
      pubsubSdk
    )
  )

lazy val `server` = (project in file("service/core/pubsub/server"))
  .settings(name := "lagom-google-pubsub-broker")
  .settings(
    libraryDependencies ++= Seq(
      slf4j,
      pubsubSdk,
      lagomApi,
      lagomPersistenceCore
    )
  )
  .dependsOn(`client`)