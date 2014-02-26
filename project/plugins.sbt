//Conflicts with main plugin in ~/.sbt/plugins/gpg.sbt
//addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.2.0")

resolvers += Resolver.url("sbt-plugin-releases", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
