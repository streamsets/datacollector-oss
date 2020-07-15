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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.event.binding.MessagingDtoJsonMapper;
import com.streamsets.datacollector.event.binding.MessagingJsonToFromDto;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.event.json.PipelineStartEventJson;
import com.streamsets.datacollector.execution.alerts.EmailNotifier;
import com.streamsets.datacollector.execution.alerts.WebHookNotifier;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.lib.security.acl.dto.Acl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract  class AbstractRunner implements Runner {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRunner.class);

  public static final String RUNTIME_PARAMETERS_ATTR = "RUNTIME_PARAMETERS";
  public static final String INTERCEPTOR_CONFIGS_ATTR = "INTERCEPTOR_CONFIGS";
  public static final String ANTENNA_DOCTOR_MESSAGES_ATTR = "ANTENNA_DOCTOR_MESSAGES";
  public static final String ERROR_STACKTRACE_ATTR = "ERROR_STACKTRACE";
  public static final String ERROR_MESSAGE_ATTR = "ERROR_MESSAGE";

  private final String name;
  private final String rev;
  private final HashMap<String, ConnectionConfiguration> connections;

  @Inject AclStoreTask aclStoreTask;
  @Inject EventListenerManager eventListenerManager;
  @Inject PipelineStoreTask pipelineStore;
  @Inject PipelineStateStore pipelineStateStore;
  @Inject StageLibraryTask stageLibrary;
  @Inject CredentialStoresTask credentialStoresTask;
  @Inject RuntimeInfo runtimeInfo;
  @Inject BuildInfo buildInfo;
  @Inject Configuration configuration;

  // Start Pipeline Context that was used during last start() and will be reused on pipeline retry
  private StartPipelineContext startPipelineContext;

  public AbstractRunner(String name, String rev) {
    this.name = name;
    this.rev = rev;
    this.connections = new HashMap<>();
  }

  protected AbstractRunner(
      String name,
      String rev,
      RuntimeInfo runtimeInfo,
      BuildInfo buildInfo,
      Configuration configuration,
      PipelineStateStore pipelineStateStore,
      PipelineStoreTask pipelineStore,
      StageLibraryTask stageLibrary,
      EventListenerManager eventListenerManager,
      AclStoreTask aclStore
  ) {
    this(name, rev);
    this.aclStoreTask = aclStore;
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
    this.buildInfo = buildInfo;
    this.pipelineStateStore = pipelineStateStore;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
    this.eventListenerManager = eventListenerManager;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getRev() {
    return rev;
  }

  @Override
  public Map<String, ConnectionConfiguration> getConnections() {
    return connections;
  }

  protected AclStoreTask getAclStore() {
    return aclStoreTask;
  }

  protected EventListenerManager getEventListenerManager() {
    return eventListenerManager;
  }

  protected PipelineStoreTask getPipelineStore() {
    return pipelineStore;
  }

  protected PipelineStateStore getPipelineStateStore() {
    return pipelineStateStore;
  }

  protected StageLibraryTask getStageLibrary() {
    return stageLibrary;
  }

  protected CredentialStoresTask getCredentialStores() {
    return credentialStoresTask;
  }

  protected RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  protected BuildInfo getBuildInfo() {
    return buildInfo;
  }

  protected Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public PipelineConfiguration getPipelineConfiguration(String user) throws PipelineException {
    return getPipelineConf(getName(), getRev(), user);
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return pipelineStateStore.getState(getName(), getRev());
  }

  @Override
  public List<PipelineState> getHistory() throws PipelineStoreException {
    return pipelineStateStore.getHistory(getName(), getRev(), false);
  }

  @Override
  public void deleteHistory() {
    pipelineStateStore.deleteHistory(getName(), getRev());
  }

  @Override
  public Map<String, Object> createStateAttributes() throws PipelineStoreException {
    Map<String, Object> attributes = new HashMap<>(getState().getAttributes());
    if(startPipelineContext != null) {
      attributes.put(RUNTIME_PARAMETERS_ATTR, startPipelineContext.getRuntimeParameters());
    }

    List<String> interceptors = new ArrayList<>();
    try {
      for(PipelineStartEvent.InterceptorConfiguration config : startPipelineContext.getInterceptorConfigurations()) {
        interceptors.add(MessagingJsonToFromDto.INSTANCE.serialize(
          MessagingDtoJsonMapper.INSTANCE.toInterceptorConfigurationJson(config)
        ));
      }
    } catch (JsonProcessingException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0214, e);
    }
    attributes.put(INTERCEPTOR_CONFIGS_ATTR, interceptors);
    return attributes;
  }

  /**
   * Variant of createStateAttributes that will remove attributes that should not be carried over from past executions.
   */
  public Map<String, Object> createNewStateAttributes() throws PipelineStoreException {
    Map<String, Object> attributes = createStateAttributes();
    attributes.remove(ANTENNA_DOCTOR_MESSAGES_ATTR);
    attributes.remove(ERROR_STACKTRACE_ATTR);
    attributes.remove(ERROR_MESSAGE_ATTR);
    return attributes;
  }

  protected void setStartPipelineContext(StartPipelineContext context) {
    this.startPipelineContext = context;
  }

  @VisibleForTesting
  public StartPipelineContext getStartPipelineContext() {
    return startPipelineContext;
  }

  @VisibleForTesting
  public StartPipelineContext loadStartPipelineContextFromState(String user) throws PipelineStoreException {
    PipelineState pipelineState = getState();
    Map<String, Object> attributes = pipelineState.getAttributes();
    Map<String, Object> runtimeParameters = null;
    List<PipelineStartEvent.InterceptorConfiguration> interceptors = null;

    if (attributes != null && attributes.containsKey(RUNTIME_PARAMETERS_ATTR)) {
      runtimeParameters = (Map<String, Object>) attributes.get(RUNTIME_PARAMETERS_ATTR);
    }

    if(attributes != null && attributes.containsKey(INTERCEPTOR_CONFIGS_ATTR)) {
      TypeReference<PipelineStartEventJson.InterceptorConfigurationJson> typeRef = new TypeReference<PipelineStartEventJson.InterceptorConfigurationJson>() {
      };

      try {
        interceptors = new ArrayList<>();
        for (String jsonConfig : (List<String>) attributes.get(INTERCEPTOR_CONFIGS_ATTR)) {
          PipelineStartEventJson.InterceptorConfigurationJson config = MessagingJsonToFromDto.INSTANCE.deserialize(jsonConfig, typeRef);
          interceptors.add(MessagingDtoJsonMapper.INSTANCE.asInterceptorConfigurationDto(config));
        }
      } catch (IOException e) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0214, e);
      }
    }

    StartPipelineContext newContext = new StartPipelineContextBuilder(user)
      .withRuntimeParameters(runtimeParameters)
      .withInterceptorConfigurations(interceptors)
      .build();

    setStartPipelineContext(newContext);
    return newContext;
  }

  protected PipelineConfiguration getPipelineConf(String name, String rev, String user) throws PipelineException {
    PipelineConfiguration load = pipelineStore.load(name, rev);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(
        stageLibrary,
        buildInfo,
        name,
        load,
        user,
        connections
    );
    PipelineConfiguration validate = validator.validate();
    if(validator.getIssues().hasIssues()) {
      LOG.error("Can't run pipeline due to issues: {}", validator.getIssues().getIssueCount());
      for(Issue issue : validator.getIssues().getIssues()) {
        LOG.error("Pipeline validation error: {}", issue);
      }
      throw new PipelineRunnerException(ContainerError.CONTAINER_0158, validator.getIssues().getIssues().size());
    }
    return validate;
  }

  protected Acl getAcl(String name) throws PipelineException {
    return aclStoreTask.getAcl(name);
  }

  protected void registerEmailNotifierIfRequired(
      PipelineConfigBean pipelineConfigBean,
      String pipelineId,
      String pipelineTitle,
      String rev
  ) {
    //remove existing email notifier
    StateEventListener toRemove = null;
    List<StateEventListener> stateEventListenerList = eventListenerManager.getStateEventListenerList();
    for(StateEventListener s : stateEventListenerList) {
      if(s instanceof EmailNotifier &&
        ((EmailNotifier)s).getPipelineId().equals(pipelineId) &&
        ((EmailNotifier)s).getRev().equals(rev)) {
        toRemove = s;
      }
    }

    if(toRemove != null) {
      eventListenerManager.removeStateEventListener(toRemove);
    }

    //register new one if required
    if(pipelineConfigBean.notifyOnStates != null && !pipelineConfigBean.notifyOnStates.isEmpty() &&
      pipelineConfigBean.emailIDs != null && !pipelineConfigBean.emailIDs.isEmpty()) {
      Set<String> states = new HashSet<>();
      for(com.streamsets.datacollector.config.PipelineState s : pipelineConfigBean.notifyOnStates) {
        states.add(s.name());
      }
      EmailNotifier emailNotifier = new EmailNotifier(
          pipelineId,
          pipelineTitle,
          rev,
          runtimeInfo,
          new EmailSender(configuration),
          pipelineConfigBean.emailIDs,
          states
      );
      eventListenerManager.addStateEventListener(emailNotifier);
    }
  }

  protected void registerWebhookNotifierIfRequired(
      PipelineConfigBean pipelineConfigBean,
      String pipelineId,
      String pipelineTitle,
      String rev
  ) {
    //remove existing Webhook notifier
    StateEventListener toRemove = null;
    List<StateEventListener> stateEventListenerList = eventListenerManager.getStateEventListenerList();
    for(StateEventListener s : stateEventListenerList) {
      if(s instanceof WebHookNotifier &&
          ((WebHookNotifier)s).getPipelineId().equals(pipelineId) &&
          ((WebHookNotifier)s).getRev().equals(rev)) {
        toRemove = s;
      }
    }

    if(toRemove != null) {
      eventListenerManager.removeStateEventListener(toRemove);
    }

    //register new one if required
    if(pipelineConfigBean.notifyOnStates != null && !pipelineConfigBean.notifyOnStates.isEmpty() &&
        pipelineConfigBean.webhookConfigs != null && !pipelineConfigBean.webhookConfigs.isEmpty()) {
      WebHookNotifier webHookNotifier = new WebHookNotifier(
          pipelineId,
          pipelineTitle,
          rev,
          pipelineConfigBean,
          runtimeInfo,
          startPipelineContext.getRuntimeParameters()
      );
      eventListenerManager.addStateEventListener(webHookNotifier);
    }
  }

  protected boolean isRemotePipeline() throws PipelineStoreException {
    Object isRemote = getState().getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    // remote attribute will be null for pipelines with version earlier than 1.3
    return isRemote != null && (boolean) isRemote;
  }

  protected ScheduledFuture<Void> scheduleForRetries(
      ScheduledExecutorService runnerExecutor
  ) throws PipelineStoreException {
    long delay = 0;
    long retryTimeStamp = getState().getNextRetryTimeStamp();
    long currentTime = System.currentTimeMillis();
    if (retryTimeStamp > currentTime) {
      delay = retryTimeStamp - currentTime;
    }
    Preconditions.checkNotNull(startPipelineContext, "Can't retry pipeline, previous start context was not saved");
    LOG.info("Scheduling retry in '{}' milliseconds", delay);
    return runnerExecutor.schedule(() -> {
      LOG.info("Starting the runner now");
      prepareForStart(startPipelineContext);
      start(startPipelineContext);
      return null;
    }, delay, TimeUnit.MILLISECONDS);
  }
}
