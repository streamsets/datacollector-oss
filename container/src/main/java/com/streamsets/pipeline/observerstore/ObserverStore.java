/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore;

import com.streamsets.pipeline.config.RuleDefinition;

public interface ObserverStore {

  public RuleDefinition retrieveRules(String name, String rev);

  public RuleDefinition storeRules(String pipelineName, String rev, RuleDefinition ruleDefinition);
}
