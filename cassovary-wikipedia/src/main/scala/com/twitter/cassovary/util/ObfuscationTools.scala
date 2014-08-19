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

/**
 * Utils shared by all Wikipedia dumps processing tools.
 */
object ObfuscationTools {
  def obfuscate(s: String) = {
    s.trim()
      .replaceAll("\\p{Z}", "_") // All separators to underscore
      .replaceAll("\\p{C}", "")  // Ingore all other characters
  }

  def validPageName(s: String): Boolean = {
    !s.isEmpty && specialLinkPrefixes.find(s.startsWith).isEmpty
  }

  /**
   * Special prefixes of wikipedia links that point not to a normal page.
   *
   * List created manually (no official documentation of dumps exists). May
   * need improvements in the future.
   */
  val specialLinkPrefixes = Array(
    "File:",
    "Image:",
    "User:",
    "user:",
    "d:", // discussion
    "Special:",
    "Extension:",
    "media:",
    "Special:", // special links
    "#", // link to anchor
    "{{", // talk page
    "//", // subpage (todo: can be treated other way)
    ":fr:", "fr:", // language codes
    ":es:", "es:",
    ":pt:", "pt:",
    ":de:", "de:",
    ":pl:", "pl:",
    ":it:", "it:",
    ":cn:", "cn:",
    ":en:", "en:",
    ":nl:", "nl:"
  )
}
