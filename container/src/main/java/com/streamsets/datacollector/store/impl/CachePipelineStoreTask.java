/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.store.impl;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineRevInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.pipeline.api.impl.Utils;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CachePipelineStoreTask implements PipelineStoreTask {

  private final PipelineStoreTask pipelineStore;
  private final ConcurrentMap<String, PipelineInfo> pipelineInfoMap;
  private final LockCache<String> lockCache;

  @Inject
  public CachePipelineStoreTask(PipelineStoreTask pipelineStore, LockCache<String> lockCache) {
    this.pipelineStore = pipelineStore;
    pipelineInfoMap = new ConcurrentHashMap<>();
    this.lockCache = lockCache;
  }

  @Override
  public String getName() {
    return "CachePipelineStoreTask";
  }

  @Override
  public void init() {
    pipelineStore.init();
    try {
      for (PipelineInfo info: pipelineStore.getPipelines()) {
        pipelineInfoMap.put(info.getName(), info);
      }
    } catch (PipelineStoreException e) {
      throw new RuntimeException(Utils.format("Cannot fetch list of pipelines due to: '{}'", e), e);
    }
  }

  @Override
  public void run() {
    pipelineStore.run();
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    pipelineStore.waitWhileRunning();
  }

  @Override
  public void stop() {
    pipelineStore.stop();
    pipelineInfoMap.clear();
  }

  @Override
  public Status getStatus() {
    return pipelineStore.getStatus();
  }

  @Override
  public PipelineConfiguration create(String user, String name, String description) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      PipelineConfiguration pipelineConf = pipelineStore.create(user, name, description);
      pipelineInfoMap.put(name, pipelineConf.getInfo());
      return pipelineConf;
    }
  }

  @Override
  public void delete(String name) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      pipelineStore.delete(name);
      pipelineInfoMap.remove(name);
    }
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    return Collections.unmodifiableList(new ArrayList(pipelineInfoMap.values()));
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineStoreException {
    PipelineInfo pipelineInfo = pipelineInfoMap.get(name);
    if (pipelineInfo == null) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    } else {
      return pipelineInfo;
    }
  }

  @Override
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    return pipelineStore.getHistory(name);
  }

  @Override
  public PipelineConfiguration save(String user, String name, String tag, String tagDescription,
    PipelineConfiguration pipeline) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      PipelineConfiguration pipelineConf = pipelineStore.save(user, name, tag, tagDescription, pipeline);
      pipelineInfoMap.put(name, pipelineConf.getInfo());
      return pipelineConf;
    }
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    return pipelineStore.load(name, tagOrRev);
  }

  @Override
  public boolean hasPipeline(String name) {
    return pipelineInfoMap.containsKey(name);
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineStoreException {
    return pipelineStore.retrieveRules(name, tagOrRev);
  }

  @Override
  public RuleDefinitions storeRules(String pipelineName, String tag, RuleDefinitions ruleDefinitions)
    throws PipelineStoreException {
    return pipelineStore.storeRules(pipelineName, tag, ruleDefinitions);
  }

  @Override
  public boolean deleteRules(String name) throws PipelineStoreException {
    return pipelineStore.deleteRules(name);
  }

  @VisibleForTesting
  PipelineStoreTask getActualStore()  {
    return pipelineStore;
  }

  @Override
  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineStoreException {
    pipelineStore.saveUiInfo(name, rev, uiInfo);
  }

}
