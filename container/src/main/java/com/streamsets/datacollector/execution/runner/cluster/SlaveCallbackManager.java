/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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

  private final RuntimeInfo runtimeInfo;
  private final ReentrantLock callbackCacheLock;
  private final Cache<String, CallbackInfo> slaveCallbackList;

  public SlaveCallbackManager(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
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
    String sdcToken = Strings.nullToEmpty(runtimeInfo.getSDCToken());
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
}
