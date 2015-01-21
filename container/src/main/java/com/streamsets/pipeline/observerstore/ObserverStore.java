/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore;

import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.CounterDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;

import java.util.List;

public interface ObserverStore {

  public List<AlertDefinition> storeAlerts(String pipelineName, String rev, List<AlertDefinition> alerts);

  public List<AlertDefinition> retrieveAlerts(String pipelineName, String rev);

  public List<MetricsAlertDefinition> storeMetricAlerts(String pipelineName, String rev,
                                                        List<MetricsAlertDefinition> alerts);

  public List<MetricsAlertDefinition> retrieveMetricAlerts(String pipelineName, String rev);

  public List<SamplingDefinition> retrieveSamplingDefinitions(String name, String rev);

  public List<SamplingDefinition> storeSamplingDefinitions(String pipelineName, String rev,
                                                           List<SamplingDefinition> alerts);

  public List<CounterDefinition> storeCounters(String pipelineName, String rev, List<CounterDefinition> alerts);

  public List<CounterDefinition> retrieveCounters(String pipelineName, String rev);
}
