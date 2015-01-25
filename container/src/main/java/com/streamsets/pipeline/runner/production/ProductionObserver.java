/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.util.Configuration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ProductionObserver implements Observer {

  private RuleDefinition ruleDefinition;
  private BlockingQueue<ProductionObserveRequest> observeRequests;

  public ProductionObserver(BlockingQueue<ProductionObserveRequest> observeRequests) {
    this.observeRequests = observeRequests;
  }

  @Override
  public void configure(Configuration conf) {

   }

  @Override
  public boolean isObserving(Stage.Info info) {
    //For now, if some alert configuration is specified, return true.
    //Need to optimise this
    if(ruleDefinition != null) {
      if(ruleDefinition.getMetricsAlertDefinitions() != null &&
        !ruleDefinition.getMetricsAlertDefinitions().isEmpty()) {
        return true;
      }
      if(ruleDefinition.getMetricDefinitions() != null && !ruleDefinition.getMetricDefinitions().isEmpty()) {
        return true;
      }
      if(ruleDefinition.getSamplingDefinitions() != null && !ruleDefinition.getSamplingDefinitions().isEmpty()) {
        return true;
      }
      if(ruleDefinition.getAlertDefinitions() != null && !ruleDefinition.getAlertDefinitions().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void observe(Pipe pipe, Map<String, List<Record>> snapshot) {
    observeRequests.offer(new ProductionObserveRequest(ruleDefinition, snapshot));
   }

  public void setRuleDefinition(RuleDefinition ruleDefinition) {
    this.ruleDefinition = ruleDefinition;
  }
}
