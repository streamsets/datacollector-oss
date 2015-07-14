/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineRevInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CachePipelineStoreTask implements PipelineStoreTask {

  private final PipelineStoreTask pipelineStore;
  private final Map<String, PipelineInfo> pipelineInfoMap;

  @Inject
  public CachePipelineStoreTask(PipelineStoreTask pipelineStore) {
    this.pipelineStore = pipelineStore;
    pipelineInfoMap = new HashMap<>();
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
  public synchronized PipelineConfiguration create(String user, String name, String description) throws PipelineStoreException {
    PipelineConfiguration pipelineConf =  pipelineStore.create(user, name, description);
    pipelineInfoMap.put(name, pipelineConf.getInfo());
    return pipelineConf;
  }

  @Override
  public synchronized void delete(String name) throws PipelineStoreException {
    pipelineStore.delete(name);
    pipelineInfoMap.remove(name);
  }

  @Override
  public synchronized List<PipelineInfo> getPipelines() throws PipelineStoreException {
    return Collections.unmodifiableList(new ArrayList(pipelineInfoMap.values()));
  }

  @Override
  public synchronized PipelineInfo getInfo(String name) throws PipelineStoreException {
    if (!pipelineInfoMap.containsKey(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    } else {
      return pipelineInfoMap.get(name);
    }
  }

  @Override
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    return pipelineStore.getHistory(name);
  }

  @Override
  public synchronized PipelineConfiguration save(String user, String name, String tag, String tagDescription,
    PipelineConfiguration pipeline) throws PipelineStoreException {
    PipelineConfiguration pipelineConf =  pipelineStore.save(user, name, tag, tagDescription, pipeline);
    pipelineInfoMap.put(PipelineDirectoryUtil.getEscapedPipelineName(name), pipelineConf.getInfo());
    return pipelineConf;
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    return pipelineStore.load(name, tagOrRev);
  }

  @Override
  public synchronized boolean hasPipeline(String name) {
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

}
