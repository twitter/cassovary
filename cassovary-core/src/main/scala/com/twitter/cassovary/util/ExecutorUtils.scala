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

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.util.{Future, FuturePool, Duration}
import java.util.ArrayList
import java.util.concurrent.{Future => JFuture, _}
import java.util.{List => JList}

/**
 * Utility class for unbounded and bounded parallel execution
 */
object ExecutorUtils {

  private val statsReceiver = DefaultStatsReceiver

  private def rejectedExecutionHandlerFactory(name: String,
      handler: => Unit): RejectedExecutionHandler = {
    new RejectedExecutionHandler {
      def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
        handler
      }
    }
  }

  /**
   * Creates an `java.util.concurrent.ExecutorService` with bounded work queue and fixed lower
   * and upper bounds on the thread pool size used to service the work queue.  The returned executor
   * service will export stats monitoring queue depth, active thread count and rejections due to
   * queue full conditions.
   *
   * <p>The supplied config map requires the following configuration keys:
   * <ul>
   * <li>max_work_queue_depth
   * <li>min_num_worker_threads
   * <li>max_num_worker_threads
   * <li>max_worker_idle_time_millis
   * </ul>
   */
  def createBoundedExecutorService(name: String,
                                   config: String => Int)
                                  (rejectedExecutionHandler: => Unit): ExecutorService = {

    createBoundedExecutorService(name,
        config("max_work_queue_depth"),
        config("min_num_worker_threads"),
        config("max_num_worker_threads"),
        Duration.fromTimeUnit(config("max_worker_idle_time_millis").toInt,
        TimeUnit.MILLISECONDS))(rejectedExecutionHandler)
  }

  /**
   * Creates an `java.util.concurrent.ExecutorService` with bounded work queue and fixed lower
   * and upper bounds on the thread pool size used to service the work queue.  The returned executor
   * service will export stats monitoring queue depth, active thread count and rejections due to
   * queue full conditions.
   */
  def createBoundedExecutorService(name: String,
                                   maxWorkQueueDepth: Int,
                                   minThreads: Int,
                                   maxThreads: Int,
                                   maxIdleTime: Duration)
                                  (rejectedExecutionHandler: => Unit): ExecutorService = {

    val taskQueue = new LinkedBlockingQueue[Runnable](maxWorkQueueDepth)

    val threadPoolExecutor = new ThreadPoolExecutor(
        minThreads,
        maxThreads,
        maxIdleTime.inMillis,
        TimeUnit.MILLISECONDS,
        taskQueue,
        createThreadFactory(name),
        rejectedExecutionHandlerFactory(name, rejectedExecutionHandler))
    statsReceiver.addGauge(name + "_queue_depth") { taskQueue.size }
    createPoolStats(name, threadPoolExecutor)

    threadPoolExecutor
  }

  private def createThreadFactory(name: String) = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "[%d]").build
  }

  private def createPoolStats(name: String, threadPoolExecutor: ThreadPoolExecutor): Unit = {
    statsReceiver.addGauge(name + "_threads_active") { threadPoolExecutor.getActiveCount }
    statsReceiver.addGauge(name + "_max_threads_active") { threadPoolExecutor.getLargestPoolSize }
  }

  /**
   * Farm out separate threads using an ExecutorService to perform work on inputs
   * @param executorService the ExecutorService to use for parallelization
   * @param inputs a sequence of inputs to work on
   * @param work the work to apply to the inputs
   */
  def parallelWork[T, A](executorService: ExecutorService, inputs: Seq[T],
      work: T => A): JList[JFuture[A]] = {
    val workTasks = {
      val tasks = new ArrayList[Callable[A]](inputs.size)
      inputs foreach { input =>
        tasks.add(new Callable[A]() {
          def call(): A = {
            work(input)
          }
        })
      }
      tasks
    }

    executorService.invokeAll(workTasks)
  }

  /**
   * Farm out separate threads using an ExecutorService to perform
   * work on inputs. Unlike [[ExecutorUtils.parallelWork]], no values
   * are returned. If an exception occurs in a worker thread, it is
   * rethrown on the calling thread.
   *  @param executorService the ExecutorService to use for parallelization
   *  @param inputs a sequence of inputs to work on
   *  @param work the work to apply to the inputs
   */
  def parallelForEach[T, A](executorService: ExecutorService, inputs: Seq[T],
      work: T => A): Unit = {
    import scala.collection.JavaConversions._
    parallelWork(executorService, inputs, work) foreach { _.get }
  }
}

/**
 * Future pool that executes task asynchronously with bounded parallelism level.
 */
class BoundedFuturePool(originalFuturePool: FuturePool, parallelismLevel: Int) extends FuturePool {

  private[this] lazy val parallelismSemaphore = new AsyncSemaphore(parallelismLevel)

  override def apply[T](f: => T): Future[T] = {
    parallelismSemaphore.acquireAndRun {
      originalFuturePool.apply(f)
    }
  }
}
