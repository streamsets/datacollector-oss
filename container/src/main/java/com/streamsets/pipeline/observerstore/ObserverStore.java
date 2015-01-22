/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.RuleDefinition;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface ObserverStore {

  public RuleDefinition retrieveRules(String name, String rev);

  public RuleDefinition storeRules(String pipelineName, String rev, RuleDefinition ruleDefinition);

  public void storeSampledRecords(String pipelineName, String rev, Map<String, List<Record>> errorRecords);

  public void register(String pipelineName, String rev);

  public void deleteSampledRecords(String pipelineName, String rev);

  public InputStream getSampledRecords(String pipelineName, String rev);

}
