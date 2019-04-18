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
package com.streamsets.datacollector.execution.runner.cluster;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class SlaveCallbackManager {

  private static final Logger LOG = LoggerFactory.getLogger(SlaveCallbackManager.class);

  private final Map<CallbackObjectType, ReentrantLock> callbackTypeToCacheLock;
  private final Map<CallbackObjectType, Cache<String, CallbackInfo>> slaveCallbackList;
  private String clusterToken;

  public SlaveCallbackManager() {
    this.callbackTypeToCacheLock = ImmutableMap.of(CallbackObjectType.METRICS, new ReentrantLock(), CallbackObjectType.ERROR, new ReentrantLock());
    //Metrics will still expire after a minute
    Cache<String, CallbackInfo> slaveMetricsCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
    //Error cache entries will be invalidated immediately after visiting them in the ClusterRunner
    Cache<String, CallbackInfo> slaveerrorCache = CacheBuilder.newBuilder().build();
    this.slaveCallbackList = ImmutableMap.of(CallbackObjectType.METRICS, slaveMetricsCache, CallbackObjectType.ERROR, slaveerrorCache);
  }

  private ReentrantLock getLock(CallbackObjectType callbackObjectType) {
    ReentrantLock reentrantLock = callbackTypeToCacheLock.get(callbackObjectType);
    Utils.checkNotNull(reentrantLock, "Invalid callbackTypeToCacheLock list initialization");
    return reentrantLock;
  }

  private Cache<String, CallbackInfo> getCache(CallbackObjectType callbackObjectType) {
    Cache<String, CallbackInfo> slaveCallbackCache = slaveCallbackList.get(callbackObjectType);
    Utils.checkNotNull(slaveCallbackCache, "Invalid SlaveCallBack list initialization");
    return slaveCallbackCache;
  }

  public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
    List<CallbackInfo> callbackInfoSet;
    ReentrantLock lock = getLock(callbackObjectType);
    lock.lock();
    try {
      callbackInfoSet = new ArrayList<>(getCache(callbackObjectType).asMap().values());
    } finally {
      lock.unlock();
    }
    return callbackInfoSet;
  }

  public Map<String, Object> updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    String sdcToken = Strings.nullToEmpty(this.clusterToken);
    if (sdcToken.equals(callbackInfo.getSdcClusterToken()) &&
      !RuntimeInfo.UNDEF.equals(callbackInfo.getSdcURL())) {
      ReentrantLock lock = getLock(callbackInfo.getCallbackObjectType());
      lock.lock();
      try {
       getCache(callbackInfo.getCallbackObjectType()).put(callbackInfo.getSdcURL(), callbackInfo);
      } finally {
        lock.unlock();
      }
    } else {
      LOG.warn("SDC Cluster token not matched");
    }
    return null;
  }

  public void clearSlaveList() {
    List<CallbackObjectType> slaveCallbackObjectTypes = ImmutableList.of(
        CallbackObjectType.METRICS,
        CallbackObjectType.ERROR
    );
    for (CallbackObjectType callbackObjectType : slaveCallbackObjectTypes) {
      ReentrantLock lock = getLock(callbackObjectType);
      lock.lock();
      try {
        getCache(callbackObjectType).invalidateAll();
      } finally {
        lock.unlock();
      }
    }
  }


  public void clearSlaveList(CallbackObjectType callbackObjectType) {
    ReentrantLock lock = getLock(callbackObjectType);
    lock.lock();
    try {
      getCache(callbackObjectType).invalidateAll();
    } finally {
      lock.unlock();
    }
  }

  public void setClusterToken(String clusterToken) {
    this.clusterToken = clusterToken;
  }

  public String getClusterToken() {
    return clusterToken;
  }
}
