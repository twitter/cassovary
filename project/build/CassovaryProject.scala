/*
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

import sbt._
import com.twitter.sbt._

class CassovaryProject(info: ProjectInfo) extends StandardProject(info) with DefaultRepos
  with SubversionPublisher with NoisyDependencies with PublishSourcesAndJavadocs {

  // Maven repositories
  val twitterInternal = "twitter internal" at "http://binaries.local.twitter.com/maven/"
  override def subversionRepository = Some("https://svn.twitter.biz/maven")

  // library dependencies
  // note that JARs in lib/ are also pulled in, and so are not mentioned here
  val util = "com.twitter" % "util" % "1.8.5"
  val guava = "com.google.guava" % "guava" % "r06" withSources()
  val configgy = "net.lag" % "configgy" % "2.0.2"
  val ostrich = "com.twitter" % "ostrich" % "4.2.0"

  val mockito = "org.mockito" % "mockito-all" % "1.8.5" % "test" withSources()

  val specs = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test" withSources()

  override def disableCrossPaths = true
  override def compileOrder = CompileOrder.JavaThenScala
}
