addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")

resolvers += Resolver.url("sbt-plugin-releases", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
