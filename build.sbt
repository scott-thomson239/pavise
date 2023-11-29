// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.0" // your current series x.y

ThisBuild / organization := "com.scott-thomson239"
ThisBuild / organizationName := "Scott Thomson"
ThisBuild / startYear := Some(2023)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("scott-thomson239", "Scott Thomson")
)

// publish to s01.oss.sonatype.org (set to true to publish to oss.sonatype.org instead)
ThisBuild / tlSonatypeUseLegacyHost := false

// publish website from this branch
ThisBuild / tlSitePublishBranch := Some("main")

ThisBuild / scalaVersion := "3.3.0"

lazy val root = tlCrossRootProject.aggregate(core)

lazy val core = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(
    name := "pavise",
    libraryDependencies ++= Seq(
      "org.typelevel" %%% "cats-core" % "2.9.0",
      "org.typelevel" %%% "cats-effect" % "3.5.1",
      "co.fs2" %%% "fs2-core" % "3.7.0",
      "co.fs2" %%% "fs2-io" % "3.7.0",
      "co.fs2" %%% "fs2-scodec" % "3.7.0",
      "org.scodec" %%% "scodec-core" % "2.2.1",
      "org.scalameta" %%% "munit" % "1.0.0-M10" % Test,
      "org.typelevel" %%% "munit-cats-effect" % "2.0.0-M4" % Test
    ),
    scalacOptions ++= List("-Wunused:all", "-source:future")
  )

lazy val docs = project.in(file("site")).enablePlugins(TypelevelSitePlugin)
