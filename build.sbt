val PureconfigVersion     = "0.12.1"
val SparkVersion          = "2.4.3" //To be compatible with GCloud Dataproc latest image

val CatsEffectVersion     = "2.0.0"
val LogbackVersion        = "1.2.3"
val Log4catsVersion       = "1.0.0"

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.47deg",
      scalaVersion := "2.11.12"
    )),
    name := "sparksftpTest",
    version := "0.0.1",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,

    coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core"     % SparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming" % SparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % SparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % SparkVersion,
      "com.github.pureconfig" %% "pureconfig"  %  PureconfigVersion,
      "org.typelevel" %% "cats-effect" % CatsEffectVersion,
      "com.springml" % "spark-sftp_2.11" % "1.1.4",
      "io.netty" % "netty-all" % "4.1.17.Final",
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
      "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",
      "org.apache.spark" %% "spark-hive"       % SparkVersion % "test",
      "org.slf4j" % "slf4j-api" % "1.7.27",
      "ch.qos.logback"  %  "logback-classic"     % LogbackVersion,
      "ch.qos.logback"  %  "logback-core"     % LogbackVersion
    ),
    addCompilerPlugin("org.typelevel" %% "kind-projector"     % "0.10.3"),
    addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1"),
    addCompilerPlugin("org.scalamacros" % "paradise"            % "2.1.0" cross CrossVersion.full),

// uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },

   resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    pomIncludeRepository := { x => false },

    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },

    assemblyMergeStrategy in assembly := {
      case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("com", "google", xs @ _*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
      case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
      case PathList("stax", "stax-apis", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", "parquet", xs @ _*) => MergeStrategy.last
      case PathList("com", "sun", "research", xs @ _*) => MergeStrategy.last
      case PathList("javax", "ws", xs @ _*) => MergeStrategy.last
      case PathList("org", "objectweb", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", "arrow", xs @ _*) => MergeStrategy.last
      case PathList("org", "objenesis", xs @ _*) => MergeStrategy.last
      case PathList("io", "netty", xs @ _*) => MergeStrategy.first
      case PathList("javax","xml", xs @ _*) => MergeStrategy.last
      case PathList("javax","xml","stream", xs @ _*) => MergeStrategy.last
      case PathList("javax","jdo", xs @ _*) => MergeStrategy.last
      case PathList("org","datanucleus", xs @ _*) => MergeStrategy.first
      case "about.html" => MergeStrategy.rename
      case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
      case "META-INF/mailcap" => MergeStrategy.last
      case "META-INF/mimetypes.default" => MergeStrategy.last
      case "plugin.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case "git.properties" => MergeStrategy.last
      case "parquet.thrift" => MergeStrategy.last
      case "codegen/config.fmpp" => MergeStrategy.last
      case "META-INF/versions/9/module-info.class" => MergeStrategy.last
      case "plugin.xml" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)

  )
