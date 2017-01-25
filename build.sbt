
lazy val root = (project in file(".")).
  settings(
    name := """BroadcastData""",
    version := "1.0",
    scalaVersion := "2.11.8",
    unmanagedBase <<= baseDirectory { base => base / "lib" })


libraryDependencies ++= Seq(
"com.typesafe.play" %% "play-json" % "2.3.4",
"com.typesafe" % "config" % "1.3.1"
)
