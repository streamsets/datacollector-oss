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
package com.streamsets.datacollector.execution.snapshot.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineException;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CacheSnapshotStore implements SnapshotStore {

  private static final String CACHE_KAY_SEPARATOR = "::";

  private final SnapshotStore snapshotStore;
  private final LoadingCache<String, SnapshotInfo> snapshotStateCache;
  private final LockCache<String> lockCache;

  @Inject
  public CacheSnapshotStore(SnapshotStore snapshotStore, LockCache<String> lockCache) {
    this.snapshotStore = snapshotStore;
    this.lockCache = lockCache;
    snapshotStateCache = CacheBuilder.newBuilder().
      maximumSize(100).
      expireAfterAccess(10, TimeUnit.MINUTES).
      build(new CacheLoader<String, SnapshotInfo>() {
        @Override
        public SnapshotInfo load(String cacheKey) throws Exception {
          return loadSnapshotInfo(cacheKey);
        }
      });
  }

  @Override
  public SnapshotInfo create(String user, String name, String rev, String id, String label, boolean failureSnapshot) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      SnapshotInfo snapshotInfo = snapshotStore.create(user, name, rev, id, label, failureSnapshot);
      snapshotStateCache.put(getCacheKey(name, rev, id), snapshotInfo);
      return snapshotInfo;
    }
  }

  @Override
  public SnapshotInfo save(
      String name,
      String rev,
      String id,
      long batchNumber,
      List<List<StageOutput>> snapshotBatches
  ) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      try {
        SnapshotInfo snapshotInfo = getSnapshotInfoFromCache(name, rev, id);
        if (snapshotInfo == null) {
          throw new PipelineException(ContainerError.CONTAINER_0605);
        }
        SnapshotInfo updatedSnapshotInfo = snapshotStore.save(name, rev, id, batchNumber, snapshotBatches);
        snapshotStateCache.put(getCacheKey(name, rev, id), updatedSnapshotInfo);
        return updatedSnapshotInfo;
      } catch (ExecutionException e) {
        throw new PipelineException(ContainerError.CONTAINER_0600, id, name, rev, e.toString(), e);
      }
    }
  }

  @Override
  public SnapshotInfo updateLabel(String name, String rev, String id, String snapshotLabel) throws PipelineException {
    return snapshotStore.updateLabel(name, rev, id, snapshotLabel);
  }

  @Override
  public Snapshot get(String name, String rev, String id) throws PipelineException {
    return snapshotStore.get(name, rev, id);
  }

  @Override
  public SnapshotInfo getInfo(String name, String rev, String id) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      try {
        return snapshotStateCache.get(getCacheKey(name, rev, id));
      } catch (ExecutionException e) {
        return null;
      }
    }
  }

  @Override
  public List<SnapshotInfo> getSummaryForPipeline(String name, String rev) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      List<SnapshotInfo> summaryForPipeline = snapshotStore.getSummaryForPipeline(name, rev);
      for (SnapshotInfo snapshotInfo : summaryForPipeline) {
        snapshotStateCache.put(getCacheKey(name, rev, snapshotInfo.getId()), snapshotInfo);
      }
      return summaryForPipeline;
    }
  }

  @Override
  public void deleteSnapshot(String name, String rev, String id) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      snapshotStateCache.invalidate(getCacheKey(name, rev, id));
      snapshotStore.deleteSnapshot(name, rev, id);
    }
  }

  private String getCacheKey(String name, String rev, String id) {
    return name + CACHE_KAY_SEPARATOR + rev + CACHE_KAY_SEPARATOR + id;
  }

  private SnapshotInfo loadSnapshotInfo(String cacheKey) throws Exception {
    String[] split = cacheKey.split(CACHE_KAY_SEPARATOR);
    SnapshotInfo info = snapshotStore.getInfo(split[0], split[1], split[2]);
    if(info == null) {
      //Null is not a valid return value for the loading cache. Therefore throw and exception.
      //The Client is expected to interpret this exception as null.
      throw new StatusInfoNotFoundException();
    }
    return info;
  }

  private SnapshotInfo getSnapshotInfoFromCache(String name, String rev, String id) throws ExecutionException {
    SnapshotInfo snapshotInfo = null;
    try {
      snapshotInfo = snapshotStateCache.get(getCacheKey(name, rev, id));
    } catch (ExecutionException e) {
      if(e.getCause() instanceof StatusInfoNotFoundException) {
        //NO-OP
      } else {
        throw e;
      }
    }
    return snapshotInfo;
  }

  static class StatusInfoNotFoundException extends Exception {

  }
}
