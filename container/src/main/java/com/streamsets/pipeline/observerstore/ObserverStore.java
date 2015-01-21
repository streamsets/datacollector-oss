/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore;

import com.streamsets.pipeline.config.AlertDefinition;

import java.util.List;

public interface ObserverStore {

  public List<AlertDefinition> storeAlerts(String pipelineName, String rev, List<AlertDefinition> alerts);

  public List<AlertDefinition> retrieveAlerts(String pipelineName, String rev);

}
