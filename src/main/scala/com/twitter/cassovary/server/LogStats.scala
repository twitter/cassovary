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
package com.twitter.cassovary.server

import com.twitter.ostrich.admin.PeriodicBackgroundProcess
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit
import com.twitter.ostrich.stats.{StatsSummary, StatsListener, Stats}
import net.lag.logging.Logger

/**
 * Ostrich service that writes aggregate and delta stats metrics to the logger
 * @param interval interval of logging
 * @param cumulative true = since the beginning, false = within the interval
 */
class LogStats(interval: Int = 10, cumulative: Boolean = false) extends PeriodicBackgroundProcess("LogStats", Duration.fromTimeUnit(interval, TimeUnit.SECONDS)) {
  private val listener = new StatsListener(Stats)
  private val log =
    if (cumulative)
      Logger.get("agg_stats")
    else
      Logger.get("delta_stats")

  def periodic() {
    val stats: StatsSummary = if (cumulative) Stats.get else listener.get
    stats.metrics.foreach { case (k, v) =>
      log.info(k + ": " + v)
    }
  }
}