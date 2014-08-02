/*
 * Copyright 2014 Twitter, Inc.
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

package com.twitter.cassovary.util

import java.io.FileOutputStream
import java.nio.charset.StandardCharsets

trait Output {
  def outputFilename: Option[String]

  var out: Option[FileOutputStream] = None

  val charset = StandardCharsets.UTF_8

  outputFilename match {
    case Some(filename) => out = Some(new FileOutputStream(filename))
    case None => out = None
  }

  def write(s: String) {
    out match {
      case Some(o) => o.write(s.getBytes(charset))
      case None => print(s)
    }
  }

  def flush() {
    out match {
      case Some(o) => o.flush()
      case None => ()
    }
  }

  def close() {
    out match {
      case Some(o) => o.close()
      case None => ()
    }
  }

}
