/*
 * Copyright 2015 Twitter, Inc.
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

object ParseString {

  /**
   * Parse a positive integer from a string. Our custom code because it is faster
   * as per our benchmarks over java.lang.Integer.parseInt() which .toInt also uses.
   * @param s string
   * @param beg beginning offset into the string
   * @param end ending offset into the string. Must be >= beg
   * @return integer representation of `s`
   */
  def toInt(s: String, beg: Int, end: Int): Int = {
    var num = s.charAt(beg) - '0'
    var i = beg + 1
    while (i <= end) {
      num = num * 10 + s.charAt(i) - '0'
      i += 1
    }
    num
  }

  def toInt(s: String): Int = toInt(s, 0, s.length - 1)

  def toLong(s: String, beg: Int, end: Int): Long = {
    var num: Long = s.charAt(beg) - '0'
    var i = beg + 1
    while (i <= end) {
      num = num * 10 + s.charAt(i) - '0'
      i += 1
    }
    num
  }

  def toLong(s: String): Long = toLong(s, 0, s.length - 1)

  def identity(s: String, beg: Int, end: Int): String = {
    s.substring(beg, end + 1)
  }

  def identity(s: String): String = s

}
