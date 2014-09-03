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

package com.twitter.cassovary

import java.io.{Writer, OutputStreamWriter, PrintWriter}

package object util {

  /**
   * Charset used for all output.
   */
  val outputCharset = "UTF-8"

  /**
   * Char used to split outputed values.
   */
  val splitChar = " "

  /**
   * Creates writer for either file (when `out` is defined) or
   * standard output if `out` is None.
   */
  def writerFor(out: Option[String]): Writer =
    out match {
      case Some(filename) => new PrintWriter(filename, outputCharset)
      case None => new OutputStreamWriter(System.out)
    }
}
