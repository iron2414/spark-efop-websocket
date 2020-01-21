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
package org.apache.spark.status.api.v1

import scala.collection.JavaConverters._

import java.util.Date
import org.apache.spark.scheduler._

class WebsocketSparkListener extends SparkListenerInterface {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = { }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    WebsocketApplicationResource.broadcast(
      new TaskData(
        taskId = taskStart.taskInfo.taskId,
        index = taskStart.taskInfo.index,
        attempt = taskStart.taskInfo.attemptNumber,
        launchTime = new Date(taskStart.taskInfo.launchTime),
        resultFetchStart = if (taskStart.taskInfo.gettingResult) {
          Some(new Date(taskStart.taskInfo.gettingResultTime))
        } else {
          None
        },
        duration = None,
        executorId = taskStart.taskInfo.executorId,
        host = taskStart.taskInfo.host,
        status = taskStart.taskInfo.status,
        taskLocality = taskStart.taskInfo.taskLocality.toString,
        speculative = taskStart.taskInfo.speculative,
        accumulatorUpdates = Seq.empty,
        errorMessage = None,
        taskMetrics = null,
        executorLogs = null,
        schedulerDelay = 0L,
        gettingResultTime = 0L)
    )
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    ???
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    WebsocketApplicationResource.broadcast(
      new TaskData(
        taskId = taskEnd.taskInfo.taskId,
        index = taskEnd.taskInfo.index,
        attempt = taskEnd.taskInfo.attemptNumber,
        launchTime = new Date(taskEnd.taskInfo.launchTime),
        resultFetchStart = if (taskEnd.taskInfo.gettingResult) {
          Some(new Date(taskEnd.taskInfo.gettingResultTime))
        } else {
          None
        },
        duration = if (taskEnd.taskInfo.duration > 0L) Some(taskEnd.taskInfo.duration) else None,
        executorId = taskEnd.taskInfo.executorId,
        host = taskEnd.taskInfo.host,
        status = taskEnd.taskInfo.status,
        taskLocality = taskEnd.taskInfo.taskLocality.toString,
        speculative = taskEnd.taskInfo.speculative,
        accumulatorUpdates = Seq.empty,
        errorMessage = Some(taskEnd.reason.toString),
        taskMetrics = Some(new TaskMetrics(
          taskEnd.taskMetrics.executorDeserializeTime,
          taskEnd.taskMetrics.executorDeserializeCpuTime,
          taskEnd.taskMetrics.executorRunTime,
          taskEnd.taskMetrics.executorCpuTime,
          taskEnd.taskMetrics.resultSize,
          taskEnd.taskMetrics.jvmGCTime,
          taskEnd.taskMetrics.resultSerializationTime,
          taskEnd.taskMetrics.memoryBytesSpilled,
          taskEnd.taskMetrics.diskBytesSpilled,
          taskEnd.taskMetrics.peakExecutionMemory,
          new InputMetrics(
            taskEnd.taskMetrics.inputMetrics.bytesRead,
            taskEnd.taskMetrics.inputMetrics.recordsRead),
          new OutputMetrics(
            taskEnd.taskMetrics.outputMetrics.bytesWritten,
            taskEnd.taskMetrics.outputMetrics.recordsWritten),
          new ShuffleReadMetrics(
            taskEnd.taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
            taskEnd.taskMetrics.shuffleReadMetrics.localBlocksFetched,
            taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime,
            taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead,
            taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
            taskEnd.taskMetrics.shuffleReadMetrics.localBytesRead,
            taskEnd.taskMetrics.shuffleReadMetrics.recordsRead),
          new ShuffleWriteMetrics(
            taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten,
            taskEnd.taskMetrics.shuffleWriteMetrics.writeTime,
            taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten
          )
        )),
        executorLogs = null,
        schedulerDelay = 0L,
        gettingResultTime = 0L)
    )
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
                                      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
                                        executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onStageExecutorMetrics(
                                       executorMetrics: SparkListenerStageExecutorMetrics): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
                                      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }

  def onExecutorBlacklistedForStage(
                                     executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = { }

  def onNodeBlacklistedForStage(
                                 nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = { }

  override def onExecutorUnblacklisted(
                                        executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }

  override def onNodeBlacklisted(
                                  nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }

  override def onNodeUnblacklisted(
                                    nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onSpeculativeTaskSubmitted(
                                           speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = { }
}