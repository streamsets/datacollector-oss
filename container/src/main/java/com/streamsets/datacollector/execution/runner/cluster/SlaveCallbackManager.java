/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.execution.runner.cluster;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.main.RuntimeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class SlaveCallbackManager {

  private static final Logger LOG = LoggerFactory.getLogger(SlaveCallbackManager.class);

  private final ReentrantLock callbackCacheLock;
  private final Cache<String, CallbackInfo> slaveCallbackList;
  private String clusterToken;

  public SlaveCallbackManager() {
    this.callbackCacheLock = new ReentrantLock();
    this.slaveCallbackList = CacheBuilder.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build();
  }

  public Collection<CallbackInfo> getSlaveCallbackList() {
    List<CallbackInfo> callbackInfoSet;
    callbackCacheLock.lock();
    try {
      callbackInfoSet = new ArrayList<>(slaveCallbackList.asMap().values());
    } finally {
      callbackCacheLock.unlock();
    }
    return callbackInfoSet;
  }

  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    String sdcToken = Strings.nullToEmpty(this.clusterToken);
    if (sdcToken.equals(callbackInfo.getSdcClusterToken()) &&
      !RuntimeInfo.UNDEF.equals(callbackInfo.getSdcURL())) {
      callbackCacheLock.lock();
      try {
        slaveCallbackList.put(callbackInfo.getSdcURL(), callbackInfo);
      } finally {
        callbackCacheLock.unlock();
      }
    } else {
      LOG.warn("SDC Cluster token not matched");
    }
  }

  public void clearSlaveList() {
    callbackCacheLock.lock();
    try {
      slaveCallbackList.invalidateAll();
    } finally {
      callbackCacheLock.unlock();
    }
  }

  public void setClusterToken(String clusterToken) {
    this.clusterToken = clusterToken;
  }

  public String getClusterToken() {
    return clusterToken;
  }
}
