/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils}

/**
 * Runs a thread pool that deserializes and remotely fetches (if necessary) task results.
 */
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends Logging {

  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)

  // Exposed for testing.
  protected val getTaskResultExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(THREADS, "task-result-getter")

  // Exposed for testing.
  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }

  protected val taskResultSerializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.serializer.newInstance()
    }
  }

  //TODO task成功执行完成后调用
  def enqueueSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      serializedData: ByteBuffer): Unit = {
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
          //TODO 获取Task的执行结果
          val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
            case directResult: DirectTaskResult[_] =>
              if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                return
              }
              // deserialize "value" without holding any lock so that it won't block other threads.
              // We should call it here, so that when it's called again in
              // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
              directResult.value(taskResultSerializer.get())
              (directResult, serializedData.limit())
            case IndirectTaskResult(blockId, size) =>
            if (!taskSetManager.canFetchMoreResults(size)) {
              // dropped by executor if size is larger than maxResultSize
              //TODO 如果Task的结果过大（超过1g）则BlockManager中移除这个block
              sparkEnv.blockManager.master.removeBlock(blockId)
                return
              }
              logDebug("Fetching indirect task result for TID %s".format(tid))
              //TODO 如果可以获取到Result 那么久去获取
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (!serializedTaskResult.isDefined) {
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, TaskResultLost)
                return
              }
              //TODO 获取到结果后反序列化
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get.toByteBuffer)
              // force deserialization of referenced value
              deserializedResult.value(taskResultSerializer.get())
              sparkEnv.blockManager.master.removeBlock(blockId)
              (deserializedResult, size)
          }

          // Set the task result size in the accumulator updates received from the executors.
          // We need to do this here on the driver because if we did this on the executors then
          // we would have to serialize the result again after updating the size.
          result.accumUpdates = result.accumUpdates.map { a =>
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              val acc = a.asInstanceOf[LongAccumulator]
              assert(acc.sum == 0L, "task result size should not have been set on the executors")
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }
          //TODO 由TaskSchedulerImpl处理任务成功的消息 在该方法中 由TaskSetManager标记该Task为成功的并通知DAGScheduler该Task成功
          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          // Matching NonFatal so we don't catch the ControlThrowable from the "return" above.
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            taskSetManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })
  }
//TODO Task失败后调用
  def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer) {
    var reason : TaskFailedReason = UnknownReason
    try {
      getTaskResultExecutor.execute(new Runnable {
        override def run(): Unit = Utils.logUncaughtExceptions {
          val loader = Utils.getContextOrSparkClassLoader
          try {
            if (serializedData != null && serializedData.limit() > 0) {
              reason = serializer.get().deserialize[TaskFailedReason](
                serializedData, loader)
            }
          } catch {
            case cnd: ClassNotFoundException =>
              // Log an error but keep going here -- the task failed, so not catastrophic
              // if we can't deserialize the reason.
              logError(
                "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
            case ex: Exception => // No-op
          } finally {
            // If there's an error while deserializing the TaskEndReason, this Runnable
            // will die. Still tell the scheduler about the task failure, to avoid a hang
            // where the scheduler thinks the task is still running.
            //TODO 调用TaskSchedulerImpl的方法 由TaskSetManager通知DAGScheduler该任务失败并根据一定的判断看是否需要重新调度该Task
            scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
          }
        }
      })
    } catch {
      case e: RejectedExecutionException if sparkEnv.isStopped =>
        // ignore it
    }
  }

  def stop() {
    getTaskResultExecutor.shutdownNow()
  }
}
