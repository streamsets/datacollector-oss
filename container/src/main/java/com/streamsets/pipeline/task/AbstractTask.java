/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.container.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractTask implements Task {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTask.class);
  private static final Map<Status, Set<Status>> VALID_TRANSITIONS = ImmutableMap.of(
      Status.CREATED, (Set<Status>)ImmutableSet.of(Status.INITIALIZED),
      Status.INITIALIZED, ImmutableSet.of(Status.RUNNING, Status.STOPPED),
      Status.RUNNING, ImmutableSet.of(Status.STOPPED),
      Status.STOPPED, ImmutableSet.of(Status.INITIALIZED, Status.STOPPED),
      Status.ERROR, ImmutableSet.<Status>of()
  );

  private static final String STATE_ERROR_MSG = "Current status is '{}'";
  private final String name;
  private final CountDownLatch latch;
  private volatile Status status;

  public AbstractTask(String name) {
    this.name = Preconditions.checkNotNull(name, "name cannot be null");
    status = Status.CREATED;
    latch = new CountDownLatch(1);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized void init() {
    Preconditions.checkState(VALID_TRANSITIONS.get(status).contains(Status.INITIALIZED),
                             Utils.format(STATE_ERROR_MSG, status));
    try {
      LOG.debug("Task '{}' initializing", getName());
      initTask();
      LOG.debug("Task '{}' initialized", getName());
    } catch (RuntimeException ex) {
      LOG.warn("Task '{}' failed to initialize, {}, calling stopTask() and going into ERROR", getName(),
               ex.getMessage(), ex);
      safeStop(Status.ERROR);
      throw ex;
    }
    status = Status.INITIALIZED;
  }

  @Override
  public synchronized void run() {
    Preconditions.checkState(VALID_TRANSITIONS.get(status).contains(Status.RUNNING),
                             Utils.format(STATE_ERROR_MSG, status));
    try {
      LOG.debug("Task '{}' starting", getName());
      runTask();
      LOG.debug("Task '{}' running", getName());
    } catch (RuntimeException ex) {
      LOG.warn("Task '{}' failed to start, {}, calling stopTask() and going into ERROR", getName(), ex.getMessage(),
               ex);
      safeStop(Status.ERROR);
      throw ex;
    }
    status = Status.RUNNING;
  }

  @Override
  public synchronized void stop() {
    Preconditions.checkState(VALID_TRANSITIONS.get(status).contains(Status.STOPPED),
                             Utils.format(STATE_ERROR_MSG, status));
    if (status == Status.INITIALIZED || status == Status.RUNNING) {
      boolean wasRunning = status == Status.RUNNING;
      LOG.debug("Task '{}' stopping", getName());
      safeStop(Status.STOPPED);
      if (wasRunning) {
        latch.countDown();
      }
    }
  }

  private void safeStop(Status endStatus) {
    try {
      stopTask();
      LOG.debug("Task '{}' stopped with status '{}'", getName(), status);
      status = endStatus;
    } catch (RuntimeException ex) {
      LOG.warn("Task '{}' failed to stop properly, {}", getName(), ex.getMessage(), ex);
      status = Status.ERROR;
    }
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    Preconditions.checkState(status == Status.RUNNING, Utils.format(STATE_ERROR_MSG, status));
    latch.await();
  }

  @Override
  public String toString() {
    return Utils.format("{}[status='{}']", getName(),status);
  }

  protected void initTask() {
  }

  protected void runTask() {
  }

  protected void stopTask() {
  }

}
