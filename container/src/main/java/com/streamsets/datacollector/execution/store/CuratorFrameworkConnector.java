/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.store;

import com.google.api.client.util.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * MUST BE A SINGLETON!
 */
public class CuratorFrameworkConnector {
  private static final Logger LOG = LoggerFactory.getLogger(CuratorFrameworkConnector.class);
  public static final String SERVICE_NAME = "com.streamsets.datacollector.curator";
  private static final String PIPELINE = "/pipeline";
  private static final String OFFSET = "/offset";
  private static final String PIPELINE_STATE =  "/state";
  private static final String PIPELINE_STATE_HISTORY = "/state_history";
  private static final String PIPELINE_INFO = "/info";
  private static final String PIPELINE_UI_INFO = "/ui_info";
  private static final String RULES = "/rules";

  public static final String PIPELINES = "/pipelines/";

  private final CuratorFramework curatorFramework;
  private final ScheduledExecutorService retryExecutor;
  private final String znode;
  private final InterProcessMutex mutex;
  private final String pipelinesBase;
  private final String pipelinesZnode;
  private final OnActiveListener onActiveListener;
  private AtomicBoolean isConnectionLost = new AtomicBoolean(false);
  private static final String LOCK_NODE = "/lock";

  public CuratorFrameworkConnector(String zkConnectionString, String znode, int retries, OnActiveListener onActiveListener) {
    curatorFramework = CuratorFrameworkFactory.newClient(zkConnectionString, new RetryNTimes(retries, 10000));
    ConnectionStateListener connectionListener = new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        if (!connectionState.isConnected()) {
          isConnectionLost.set(true);
        } else {
          isConnectionLost.set(false);
        }
      }
    };
    curatorFramework.getConnectionStateListenable().addListener(connectionListener);
    curatorFramework.start();
    this.znode = znode;
    pipelinesBase = znode + PIPELINES;
    pipelinesZnode = pipelinesBase.substring(0, pipelinesBase.length() - 1);
    this.mutex = new InterProcessMutex(curatorFramework, znode + LOCK_NODE);
    this.onActiveListener = onActiveListener;
    retryExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("ZK Locking Thread").build());
    retryExecutor.scheduleWithFixedDelay(new LockRetryTask(), 0, 10, TimeUnit.SECONDS);
  }

  private CuratorFramework getCurator() {
    return curatorFramework;
  }

  public boolean isPrimary() {
    return mutex.isAcquiredInThisProcess();
  }

  public List<String> getPipelines() throws ZKStoreException {
    try {
      return getCurator().getChildren().forPath(pipelinesZnode);
    } catch (Exception ex) {
      throw new ZKStoreException(ex);
    }
  }

  public String getPipeline(String uuid) throws ZKStoreException {
    return getData(getPipelineNode(uuid));
  }

  public String getPipelineState(String uuid) throws ZKStoreException {
    return getData(getPipelineStateNode(uuid));
  }

  public String getPipelineStateHistory(String uuid) throws ZKStoreException {
    return getData(getPipelineStateHistoryNode(uuid));
  }

  public String getOffset(String uuid) throws ZKStoreException {
    return getData(getOffsetNode(uuid));
  }

  public long getLastBatchTime(String uuid) {
    Stat stat = new Stat();
    try {
      getCurator().getData().storingStatIn(stat).forPath(getOffsetNode(uuid));
      return stat.getMtime();
    } catch (Exception ex) {
      return 0L;
    }
  }

  public String getRules(String uuid) throws ZKStoreException {
    return getData(getRulesNode(uuid));
  }

  public String getInfo(String uuid) throws ZKStoreException {
    return getData(getInfoNode(uuid));
  }

  public String getUiInfo(String uuid) throws ZKStoreException {
    return getData(getUiInfoNode(uuid));
  }

  private String getData(String path) throws ZKStoreException {
    byte[] data;

    try {
      data = getCurator().getData().forPath(path);
    } catch (Exception e) {
      return null;
    }

    return data != null ? dataFromZK(data) : null;
  }

  public boolean updatePipeline(String uuid, String pipelineJson)
      throws NotPrimaryException {
    return updatePathWithValue(getPipelineNode(uuid), pipelineJson);
  }

  public boolean updatePipelineState(String uuid, String pipelineState) throws NotPrimaryException {
    return updatePathWithValue(getPipelineStateNode(uuid), pipelineState);
  }

  public boolean updatePipelineStateHistory(String uuid, String pipelineStateHistory) throws NotPrimaryException {
    return updatePathWithValue(getPipelineStateHistoryNode(uuid), pipelineStateHistory);
  }

  public boolean updateUiInfo(String uuid, String pipelineState) throws NotPrimaryException {
    return updatePathWithValue(getUiInfoNode(uuid), pipelineState);
  }

  public boolean updateInfo(String uuid, String pipelineState) throws NotPrimaryException {
    return updatePathWithValue(getInfoNode(uuid), pipelineState);
  }

  public boolean updateRules(String uuid, String pipelineState) throws NotPrimaryException {
    return updatePathWithValue(getRulesNode(uuid), pipelineState);
  }

  public boolean deletePipelineState(String uuid) throws NotPrimaryException {
    return delete(getPipelineStateNode(uuid));
  }

  public boolean deletePipelineStateHistory(String uuid) throws NotPrimaryException {
    return delete(getPipelineStateHistoryNode(uuid));
  }

  public boolean deleteInfo(String uuid) throws NotPrimaryException {
    return delete(getInfoNode(uuid));
  }

  public boolean deleteUiInfo(String uuid) throws NotPrimaryException {
    return delete(getUiInfoNode(uuid));
  }

  public boolean deleteRules(String uuid) throws NotPrimaryException {
    return delete(getRulesNode(uuid));
  }

  public boolean commitOffset(String pipelineUuid, String offset) throws NotPrimaryException {
    return updatePathWithValue(getOffsetNode(pipelineUuid), offset);
  }


  private String getPipelineStateNode(String uuid) {
    return pipelinesBase + uuid + PIPELINE_STATE;
  }

  private String getPipelineStateHistoryNode(String uuid) {
    return pipelinesBase + uuid + PIPELINE_STATE_HISTORY;
  }

  private String getPipelineNode(String uuid) {
    return pipelinesBase + uuid + PIPELINE;
  }

  private String getOffsetNode(String uuid) {
    return pipelinesBase + uuid + OFFSET;
  }

  private String getRulesNode(String uuid) {
    return pipelinesBase + uuid + RULES;
  }

  private String getUiInfoNode(String uuid) {
    return pipelinesBase + uuid + PIPELINE_UI_INFO;
  }

  private String getInfoNode(String uuid) {
    return pipelinesBase + uuid + PIPELINE_INFO;
  }

  private byte[] dataToZK(String value) {
    return value.getBytes(Charsets.UTF_8);
  }

  private String dataFromZK(byte[] value) {
    return new String(value, Charsets.UTF_8);
  }

  private boolean delete(String path) throws NotPrimaryException {
    int version = getVersionForPath(path);

    if (version == -1) {
      return false;
    }
    // There is a subtle race condition here - we could have lost the lock
    // and this version might be from some other update.
    // To avoid overwriting somebody else's update, we lock again to ensure this was indeed our update
    // by checking in the transactional update that the version is the same as above

    boolean acquiredAgain = lock();
    try {
      getCurator().inTransaction().delete().withVersion(version).forPath(path).and().commit();
    } catch (Exception ex) {
      return false;
    } finally {
      if (acquiredAgain) {
        release();
      }
    }
    return true;
  }

  private int getVersionForPath(String path) throws NotPrimaryException {
    if (!isPrimary() || isConnectionLost.get()) {
      throw new NotPrimaryException();
    }
    try {
      Stat stat = getCurator().checkExists().forPath(path);
      if (stat == null) {
        return Integer.MIN_VALUE;
      }
      return stat.getVersion();
    } catch (Exception ex) {
      return -1;
    }

  }

  public void createPipeline(String uuid) throws ZKStoreException {
    try {
      getCurator().create().forPath(pipelinesBase + uuid);
    } catch (Exception ex) {
      throw new ZKStoreException(ex);
    }
  }

  private boolean updatePathWithValue(String path, String value) throws NotPrimaryException {
    int version = getVersionForPath(path);

    if (version == -1) {
      return false;
    }
    // There is a subtle race condition here - we could have lost the lock
    // and this version might be from some other update.
    // To avoid overwriting somebody else's update, we lock again to ensure this was indeed our update
    // by checking in the transactional update that the version is the same as above

    boolean acquiredAgain = lock();
    try {
      if (version == Integer.MIN_VALUE) {
        getCurator().create().forPath(path, dataToZK(value));
      } else {
        getCurator()
            .inTransaction().setData().withVersion(version).forPath(path, dataToZK(value)).and().commit();
      }
    } catch (Exception ex) {
      LOG.error("Update failed", ex);
      return false;
    } finally {
      if (acquiredAgain) {
        release();
      }
    }
    return true;
  }

  private boolean lock() throws NotPrimaryException {
    try {
      return mutex.acquire(1, TimeUnit.SECONDS);
    } catch (Exception ex) {
      throw new NotPrimaryException(ex);
    }
  }

  private void release() {
    try {
      mutex.release();
    } catch (Exception ignored) {
      // probably lost connection. ZK will release the lock.
    }
  }

  public void destroy() {
    release();
    getCurator().close();
  }

  private class LockRetryTask implements Runnable {

    @Override
    public void run() {
      if (!isPrimary() && !isConnectionLost.get()) {
        try {
          if (lock()) {
            LOG.info("Primary has failed, taking over");
            onActiveListener.onActive(CuratorFrameworkConnector.this);
          }
        } catch (Exception ignored) {
          LOG.error("error", ignored);
        }
      }
    }
  }
}
