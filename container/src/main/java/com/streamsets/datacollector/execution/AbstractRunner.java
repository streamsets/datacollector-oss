/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.alerts.AlertEventListener;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.metrics.MetricsEventListener;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.ValidationUtil;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;

import javax.inject.Inject;

public abstract  class AbstractRunner implements Runner {

  @Inject protected EventListenerManager eventListenerManager;
  @Inject protected PipelineStoreTask pipelineStore;
  @Inject protected StageLibraryTask stageLibrary;

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
}
