/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.snapshot;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.dataCollector.execution.Snapshot;
import com.streamsets.dataCollector.execution.SnapshotInfo;
import com.streamsets.dataCollector.execution.SnapshotStore;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineException;

import javax.inject.Inject;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CacheSnapshotStore implements SnapshotStore {

  private static final String CACHE_KAY_SEPARATOR = "::";

  private final FileSnapshotStore fileSnapshotStore;
  private final LoadingCache<String, SnapshotInfo> snapshotStateCache;

  @Inject
  public CacheSnapshotStore(FileSnapshotStore fileSnapshotStore) {
    this.fileSnapshotStore = fileSnapshotStore;
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
  public SnapshotInfo create(String user, String name, String rev, String id) throws PipelineRuntimeException {
    SnapshotInfo snapshotInfo = fileSnapshotStore.create(user, name, rev, id);
    snapshotStateCache.put(getCacheKey(name, rev, id), snapshotInfo);
    return snapshotInfo;
  }

  @Override
  public void save(String name, String rev, String id, List<List<StageOutput>> snapshotBatches)
    throws PipelineException {
    try {
      SnapshotInfo snapshotInfo = getSnapshotInfoFromCache(name, rev, id);
      Utils.checkState(snapshotInfo != null, "Snapshot must be created before saving");
      SnapshotInfo updatedSnapshotInfo = fileSnapshotStore.save(snapshotInfo.getUser(), name, rev, id, snapshotBatches);
      snapshotStateCache.put(getCacheKey(name, rev, id), updatedSnapshotInfo);
    } catch (ExecutionException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0600, id, name, rev, e.getMessage(), e);
    }
  }

  @Override
  public Snapshot get(String name, String rev, String id) throws PipelineException {
    try {
      SnapshotInfo snapshotInfo = getSnapshotInfoFromCache(name, rev, id);
      InputStream inputStream = fileSnapshotStore.getData(name, rev, id);
      return new SnapshotImpl(snapshotInfo, inputStream);
    } catch (ExecutionException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0600, id, name, rev, e.getMessage(), e);
    }
  }

  @Override
  public List<SnapshotInfo> getSummaryForPipeline(String name, String rev) throws PipelineRuntimeException {
    List<SnapshotInfo> summaryForPipeline = fileSnapshotStore.getSummaryForPipeline(name, rev);
    for(SnapshotInfo snapshotInfo : summaryForPipeline) {
      snapshotStateCache.put(getCacheKey(name, rev, snapshotInfo.getId()), snapshotInfo);
    }
    return summaryForPipeline;
  }

  @Override
  public void deleteSnapshot(String name, String rev, String id) throws PipelineRuntimeException {
    snapshotStateCache.invalidate(getCacheKey(name, rev, id));
    fileSnapshotStore.deleteSnapshot(name, rev, id);
  }

  private String getCacheKey(String name, String rev, String id) {
    return name + CACHE_KAY_SEPARATOR + rev + CACHE_KAY_SEPARATOR + id;
  }

  private SnapshotInfo loadSnapshotInfo(String cacheKey) throws Exception {
    String[] split = cacheKey.split(CACHE_KAY_SEPARATOR);
    SnapshotInfo info = fileSnapshotStore.getInfo(split[0], split[1], split[2]);
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
