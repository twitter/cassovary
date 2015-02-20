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
package com.twitter.cassovary.graph.labels

import scala.collection.mutable
import scala.reflect.runtime.universe._

class Labels[ID] {
  private val underlying = mutable.HashMap.empty[String, Label[ID, _]]

  def get[L : TypeTag](key: String): Option[Label[ID, L]] = {
    underlying.get(key).flatMap { v =>
      if (typeTag[L] == v.tag) Some(v.asInstanceOf[Label[ID, L]]) else None
    }
  }

  def +=[L](label: Label[ID, L]): Unit = {
    underlying += label.name -> label
  }

  def -=(key: String): Unit = {
    underlying -= key
  }

}
