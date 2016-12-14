/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.httpsource;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.http.HttpReceiverServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class AbstractHttpServerSource<R extends HttpReceiver> extends BaseSource implements OffsetCommitter {
  private final HttpConfigs httpConfigs;

  private R receiver;
  private HttpReceiverServer server;

  private long counter;
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;

  public AbstractHttpServerSource(HttpConfigs httpConfigs, R receiver) {
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
      server = new HttpReceiverServer(httpConfigs, receiver, getErrorQueue());
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

  /**
   * This implementation sleeps 1000ms, invoked from the {{@link #produce(String, int, BatchMaker)}} method
   * to delay batch runs.
   */
  protected void produceSleep() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
    }
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    produceSleep();
    // report errors  reported by the HttpReceiverServer
    errorList.clear();
    getErrorQueue().drainTo(errorList);
    for (Exception exception : errorList) {
      getContext().reportError(exception);
    }
    return "::abstracthttpserversource::" + (counter++) + System.currentTimeMillis();
  }

  @Override
  public void commit(String offset) throws StageException {
  }

}
