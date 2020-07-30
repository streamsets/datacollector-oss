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
package com.streamsets.datacollector.store.impl;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineRevInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineException;
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
  private List<PipelineInfo> samplePipelines;

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
        pipelineInfoMap.put(info.getPipelineId(), info);
      }
      samplePipelines = pipelineStore.getSamplePipelines();
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
  public PipelineConfiguration create(
      String user,
      String pipelineId,
      String pipelineTitle,
      String description,
      boolean isRemote,
      boolean draft,
      Map<String, Object> metadata
  ) throws PipelineException {
    synchronized (lockCache.getLock(pipelineId)) {
      PipelineConfiguration pipelineConf = pipelineStore
          .create(user, pipelineId, pipelineTitle, description, isRemote, draft, metadata);
      if (!draft) {
        pipelineInfoMap.put(pipelineConf.getInfo().getPipelineId(), pipelineConf.getInfo());
      }
      return pipelineConf;
    }
  }

  @Override
  public void delete(String name) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      pipelineStore.delete(name);
      pipelineInfoMap.remove(name);
    }
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    return Collections.unmodifiableList(new ArrayList<>(pipelineInfoMap.values()));
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
  public List<PipelineRevInfo> getHistory(String name) throws PipelineException {
    return pipelineStore.getHistory(name);
  }

  @Override
  public PipelineConfiguration save(String user, String name, String tag, String tagDescription,
    PipelineConfiguration pipeline, boolean encryptCredentials) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      PipelineConfiguration pipelineConf = pipelineStore.save(user, name, tag, tagDescription, pipeline, encryptCredentials);
      pipelineInfoMap.put(name, pipelineConf.getInfo());
      return pipelineConf;
    }
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineException {
    return pipelineStore.load(name, tagOrRev);
  }

  @Override
  public boolean hasPipeline(String name) {
    return pipelineInfoMap.containsKey(name);
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineException {
    return pipelineStore.retrieveRules(name, tagOrRev);
  }

  @Override
  public RuleDefinitions storeRules(
      String pipelineName,
      String tag,
      RuleDefinitions ruleDefinitions,
      boolean draft
  ) throws PipelineException {
    return pipelineStore.storeRules(pipelineName, tag, ruleDefinitions, draft);
  }

  @Override
  public boolean deleteRules(String name) throws PipelineException {
    return pipelineStore.deleteRules(name);
  }

  @VisibleForTesting
  PipelineStoreTask getActualStore()  {
    return pipelineStore;
  }

  @Override
  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineException {
    pipelineStore.saveUiInfo(name, rev, uiInfo);
  }

  @Override
  public PipelineConfiguration saveMetadata(
      String user,
      String name,
      String rev,
      Map<String, Object> metadata
  ) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      PipelineConfiguration pipelineConf = pipelineStore.saveMetadata(user, name, rev, metadata);
      pipelineInfoMap.put(name, pipelineConf.getInfo());
      return pipelineConf;
    }
  }

  @Override
  public void registerStateListener(StateEventListener stateListener) {
    pipelineStore.registerStateListener(stateListener);
  }

  @Override
  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
    return pipelineStore.isRemotePipeline(name, rev);
  }

  @Override
  public PipelineFragmentConfiguration createPipelineFragment(
      String user,
      String pipelineId,
      String pipelineTitle,
      String description,
      boolean draft
  ) throws PipelineException {
    return pipelineStore.createPipelineFragment(user, pipelineId, pipelineTitle, description, draft);
  }

  @Override
  public List<PipelineInfo> getSamplePipelines() {
    return samplePipelines;
  }

  @Override
  public PipelineEnvelopeJson loadSamplePipeline(String samplePipelineId) throws PipelineException {
    return pipelineStore.loadSamplePipeline(samplePipelineId);
  }
}
