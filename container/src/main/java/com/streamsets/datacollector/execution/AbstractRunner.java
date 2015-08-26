/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.alerts.EmailNotifier;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.ValidationUtil;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract  class AbstractRunner implements Runner {

  @Inject protected EventListenerManager eventListenerManager;
  @Inject protected PipelineStoreTask pipelineStore;
  @Inject protected StageLibraryTask stageLibrary;
  @Inject protected RuntimeInfo runtimeInfo;
  @Inject protected Configuration configuration;

  protected PipelineConfiguration getPipelineConf(String name, String rev) throws PipelineStoreException,
    PipelineRunnerException {
    PipelineConfiguration load = pipelineStore.load(name, rev);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, load);
    PipelineConfiguration validate = validator.validate();
    if(validator.getIssues().hasIssues()) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0158, ValidationUtil.getFirstIssueAsString(name,
        validator.getIssues()));
    }
    return validate;
  }

  protected void registerEmailNotifierIfRequired(PipelineConfigBean pipelineConfigBean, String name, String rev) {
    //remove existing email notifier
    StateEventListener toRemove = null;
    List<StateEventListener> stateEventListenerList = eventListenerManager.getStateEventListenerList();
    for(StateEventListener s : stateEventListenerList) {
      if(s instanceof EmailNotifier &&
        ((EmailNotifier)s).getName().equals(name) &&
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
      EmailNotifier emailNotifier = new EmailNotifier(name, rev, runtimeInfo, new EmailSender(configuration),
        pipelineConfigBean.emailIDs, states);
      eventListenerManager.addStateEventListener(emailNotifier);
    }
  }
}
