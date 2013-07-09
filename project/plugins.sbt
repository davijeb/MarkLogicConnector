resolvers += Classpaths.typesafeSnapshots

resolvers += Classpaths.typesafeResolver

resolvers += "scct-github-repository" at "http://mtkopone.github.com/scct/maven-repo"

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.1.0")

addSbtPlugin("reaktor" % "sbt-scct" % "0.2-SNAPSHOT")
