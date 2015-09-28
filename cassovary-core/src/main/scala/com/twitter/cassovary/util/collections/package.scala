package com.twitter.cassovary.util

/**
 * Created by szymonmatejczyk on 23.09.15.
 */
package object collections {
  object Implicits extends FastMap.Implicit with FastQueue.LowPriorityImplicit
}
