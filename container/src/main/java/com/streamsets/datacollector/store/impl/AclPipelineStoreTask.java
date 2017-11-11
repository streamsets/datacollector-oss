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
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineRevInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Action;
import com.streamsets.lib.security.acl.dto.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class AclPipelineStoreTask implements PipelineStoreTask {
  private static final Logger LOG = LoggerFactory.getLogger(AclPipelineStoreTask.class);
  private final PipelineStoreTask pipelineStore;
  private final AclStoreTask aclStore;
  private final UserJson currentUser;

  @Inject
  public AclPipelineStoreTask(PipelineStoreTask pipelineStore, AclStoreTask aclStore, UserJson currentUser) {
    this.pipelineStore = pipelineStore;
    this.aclStore =  aclStore;
    this.currentUser = currentUser;
  }

  @Override
  public String getName() {
    return "AclPipelineStoreTask";
  }

  @Override
  public void init() {
    pipelineStore.init();
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
      boolean draft
  ) throws PipelineException {
    PipelineConfiguration pipelineConf = pipelineStore
        .create(user, pipelineId, pipelineTitle, description, isRemote, draft);
    aclStore.createAcl(pipelineId, ResourceType.PIPELINE, System.currentTimeMillis(), user);
    return pipelineConf;
  }

  @Override
  public void delete(String name) throws PipelineException {
    aclStore.validateWritePermission(name, currentUser);
    pipelineStore.delete(name);
    aclStore.deleteAcl(name);
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    return new ArrayList<>(filterPipelineBasedOnReadAcl());
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineException {
    aclStore.validateReadPermission(name, currentUser);
    return pipelineStore.getInfo(name);
  }

  @Override
  public List<PipelineRevInfo> getHistory(String name) throws PipelineException {
    aclStore.validateReadPermission(name, currentUser);
    return pipelineStore.getHistory(name);
  }

  @Override
  public PipelineConfiguration save(
      String user,
      String name,
      String tag,
      String tagDescription,
      PipelineConfiguration pipeline
  ) throws PipelineException {
    aclStore.validateWritePermission(name, currentUser);
    return pipelineStore.save(user, name, tag, tagDescription, pipeline);
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineException {
    aclStore.validateReadPermission(name, currentUser);
    return pipelineStore.load(name, tagOrRev);
  }

  @Override
  public boolean hasPipeline(String name) throws PipelineException {
    aclStore.validateReadPermission(name, currentUser);
    return pipelineStore.hasPipeline(name);
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineException {
    aclStore.validateReadPermission(name, currentUser);
    return pipelineStore.retrieveRules(name, tagOrRev);
  }

  @Override
  public RuleDefinitions storeRules(
      String pipelineName,
      String tag,
      RuleDefinitions ruleDefinitions,
      boolean draft
  ) throws PipelineException {
    aclStore.validateWritePermission(pipelineName, currentUser);
    return pipelineStore.storeRules(pipelineName, tag, ruleDefinitions, draft);
  }

  @Override
  public boolean deleteRules(String name) throws PipelineException {
    aclStore.validateWritePermission(name, currentUser);
    return pipelineStore.deleteRules(name);
  }

  @VisibleForTesting
  PipelineStoreTask getActualStore()  {
    return pipelineStore;
  }

  @Override
  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineException {
    aclStore.validateWritePermission(name, currentUser);
    pipelineStore.saveUiInfo(name, rev, uiInfo);
  }

  @Override
  public PipelineConfiguration saveMetadata(
      String user,
      String name,
      String rev,
      Map<String, Object> metadata
  ) throws PipelineException {
    aclStore.validateWritePermission(name, currentUser);
    return pipelineStore.saveMetadata(user, name, rev, metadata);
  }

  @Override
  public void registerStateListener(StateEventListener stateListener) {
    pipelineStore.registerStateListener(stateListener);
  }

  @Override
  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
    return pipelineStore.isRemotePipeline(name, rev);
  }

  private Collection<PipelineInfo> filterPipelineBasedOnReadAcl() throws PipelineStoreException {
    return Collections2.filter(pipelineStore.getPipelines(), new Predicate<PipelineInfo>() {
      @Override
      public boolean apply(PipelineInfo pipelineInfo) {
        try {
          return aclStore.isPermissionGranted(pipelineInfo.getPipelineId(), EnumSet.of(Action.READ), currentUser);
        } catch (PipelineException e) {
          LOG.warn("Failed to validate ACL");
        }
        return false;
      }
    });
  }
}
