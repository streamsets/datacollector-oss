/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.RuleDefinition;

import java.util.List;
import java.util.Map;

public class ProductionObserveRequest {

  private final RuleDefinition ruleDefinition;
  private final Map<String, List<Record>> snapshot;

  public ProductionObserveRequest(RuleDefinition ruleDefinition, Map<String, List<Record>> snapshot) {
    this.ruleDefinition = ruleDefinition;
    this.snapshot = snapshot;
  }

  public RuleDefinition getRuleDefinition() {
    return ruleDefinition;
  }

  public Map<String, List<Record>> getSnapshot() {
    return snapshot;
  }
}
