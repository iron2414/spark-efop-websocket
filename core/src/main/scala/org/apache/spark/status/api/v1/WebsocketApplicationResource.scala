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

import java.io.IOException

import javax.websocket.server.ServerEndpoint
import javax.websocket._

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListenerInterface

@ServerEndpoint(value = "/ws/tasks")
private[v1] class WebsocketApplicationResource extends BaseAppResource {

  WebsocketApplicationResource.maybeInitializeListenerBus()

  private var session : Session = _

//  private var localHistory : mutable.HashSet[LocalHistory] = new mutable.HashSet[LocalHistory]()

  case class LocalHistory(
    taskId: Long,
    status: String
  )

  @OnOpen
  @throws[IOException]
  def onOpen(session: Session): Unit = {
    this.session = session
    WebsocketApplicationResource.connections += this
  }

//  def sendHistoryOver(): Unit = {
//    withUI(_.store.taskList(0)).foreach {
//      task =>
//        val history: LocalHistory = LocalHistory(task.taskId, task.status)
//        localHistory.add(history)
//        val jsonResult: String = new JacksonMessageWriter().mapper.writeValueAsString(task)
//
//        session.getBasicRemote.sendText(jsonResult)
//    }
//    /**
//      * There's no executor where we could send the future with the 3 sec delay,
//      * we have to sleep here.
//      */
//    Thread.sleep(3000)
//    withUI(_.store.taskList(0)).foreach {
//      task =>
//        val history: LocalHistory = LocalHistory(task.taskId, task.status)
//        if(!localHistory.contains(history)) {
//          val jsonResult: String = new JacksonMessageWriter().mapper.writeValueAsString(task)
//          session.getBasicRemote.sendText(jsonResult)
//        }}
//    localHistory.clear()
//  }

  @OnMessage
  @throws[IOException]
  def onMessage(session: Session, message: String): Unit = {
    // Handle new messages
  }

  @OnClose
  @throws[IOException]
  def onClose(session: Session): Unit = {
    WebsocketApplicationResource.connections -= this
  }

  @OnError def onError(session: Session, throwable: Throwable): Unit = {
    // Handle error
//    println(s"SOCKET ERROR" + throwable)
  }
}

object WebsocketApplicationResource {
  private[v1] var connections: Set[WebsocketApplicationResource] = Set()
  private[v1] var listener: Option[SparkListenerInterface] = None

  @throws[IOException]
  @throws[EncodeException]
  private[v1] def broadcast[T](message: T): Unit = {
    val jsonResult: String = new JacksonMessageWriter().mapper.writeValueAsString(message)
    WebsocketApplicationResource.connections.foreach((endpoint: WebsocketApplicationResource) => {
      this.synchronized {
        try {
          endpoint.session.getBasicRemote.sendObject(jsonResult)
        } catch {
          case e : Throwable => println(e)
        }
      }
    })
  }

  private[v1] def maybeInitializeListenerBus(): Unit = synchronized {
    listener.orElse {
      listener = Some(new WebsocketSparkListener())
      SparkContext.getOrCreate().addSparkListener(listener.get)
      listener
    }
  }
}