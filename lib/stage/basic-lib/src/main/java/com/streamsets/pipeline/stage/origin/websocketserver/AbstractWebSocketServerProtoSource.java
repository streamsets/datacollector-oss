/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ProtoSource;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.lib.http.HttpConfigs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class AbstractWebSocketServerProtoSource<R extends WebSocketReceiver, C extends ProtoSource.Context>
    extends BaseStage<C> {
  private final HttpConfigs httpConfigs;

  private R receiver;
  protected WebSocketReceiverServer server;

  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;

  public AbstractWebSocketServerProtoSource(HttpConfigs httpConfigs, R receiver) {
    this.httpConfigs = httpConfigs;
    this.receiver = receiver;
  }

  protected R getReceiver() {
    return receiver;
  }

  public HttpConfigs getHttpConfigs() {
    return httpConfigs;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (issues.isEmpty()) {
      errorQueue = new ArrayBlockingQueue<>(100);
      errorList = new ArrayList<>(100);
      server = new WebSocketReceiverServer(httpConfigs, receiver, getErrorQueue());
      issues.addAll(server.init(getContext()));
    }
    return issues;
  }

  @Override
  public void destroy() {
    if (server != null) {
      server.destroy();
    }
    super.destroy();
  }

  @VisibleForTesting
  BlockingQueue<Exception> getErrorQueue() {
    return errorQueue;
  }

  protected void dispatchHttpReceiverErrors(long intervalMillis) {
    if (intervalMillis > 0) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException ex) {
        getContext().reportError(ex);
      }
    }
    // report errors  reported by the HttpReceiverServer
    errorList.clear();
    getErrorQueue().drainTo(errorList);
    for (Exception exception : errorList) {
      getContext().reportError(exception);
    }
  }

}
