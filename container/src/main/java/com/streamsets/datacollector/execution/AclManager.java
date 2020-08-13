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
package com.streamsets.datacollector.execution;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class AclManager implements Manager {
  private static final Logger LOG = LoggerFactory.getLogger(AclManager.class);
  private final Manager manager;
  private final AclStoreTask aclStore;
  private final UserJson currentUser;

  public AclManager(Manager manager, AclStoreTask aclStore, UserJson currentUser) {
    this.manager = manager;
    this.aclStore = aclStore;
    this.currentUser = currentUser;
  }

  @Override
  public String getName() {
    return "AclManager";
  }

  @Override
  public void init() {
    manager.init();
  }

  @Override
  public void run() {
    manager.run();
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    manager.waitWhileRunning();
  }

  @Override
  public void stop() {
    manager.stop();
  }

  @Override
  public Status getStatus() {
    return manager.getStatus();
  }

  @Override
  public Previewer createPreviewer(
      String user,
      String name,
      String rev,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      boolean remote,
      Map<String, ConnectionConfiguration> connections
  ) throws PipelineException {
    aclStore.validateExecutePermission(name, currentUser);
    return manager.createPreviewer(user, name, rev, interceptorConfs, afterActionsFunction, remote, connections);
  }

  @Override
  public Previewer getPreviewer(String previewerId) {
    return manager.getPreviewer(previewerId);
  }

  @Override
  public Runner getRunner(String name, String rev) throws PipelineException {
    aclStore.validateReadPermission(name, currentUser);
    Runner runner = manager.getRunner(name, rev);
    return new AclRunner(runner, aclStore, currentUser);
  }

  @Override
  public List<PipelineState> getPipelines() throws PipelineException {
    return new ArrayList<>(filterPipelineBasedOnReadAcl());
  }

  @Override
  public PipelineState getPipelineState(String name, String rev) throws PipelineException {
    aclStore.validateReadPermission(name, currentUser);
    return manager.getPipelineState(name, rev);
  }

  @Override
  public boolean isPipelineActive(String name, String rev) throws PipelineException {
    aclStore.validateReadPermission(name, currentUser);
    return manager.isPipelineActive(name, rev);
  }

  @Override
  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
    return manager.isRemotePipeline(name, rev);
  }

  @Override
  public void addStateEventListener(StateEventListener listener) {
    manager.addStateEventListener(listener);
  }

  private Collection<PipelineState> filterPipelineBasedOnReadAcl() throws PipelineException {
    return Collections2.filter(manager.getPipelines(), new Predicate<PipelineState>() {
      @Override
      public boolean apply(PipelineState pipelineState) {
        try {
          return aclStore.isPermissionGranted(pipelineState.getPipelineId(), EnumSet.of(Action.READ), currentUser);
        } catch (PipelineException e) {
          LOG.warn("Failed to validate ACL");
        }
        return false;
      }
    });
  }
}
