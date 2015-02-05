/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.Pipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProductionObserver implements Observer {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionObserver.class);

  private BlockingQueue<Object> observeRequests;
  private RulesConfigurationChangeRequest currentConfig;
  private volatile RulesConfigurationChangeRequest newConfig;

  public ProductionObserver(BlockingQueue<Object> observeRequests) {
    this.observeRequests = observeRequests;
  }

  @Override
  public void reconfigure() {
    if(currentConfig != newConfig){
      this.currentConfig = this.newConfig;
      boolean offered = false;
      LOG.debug("Reconfiguring observer");
      while (!offered) {
        offered = observeRequests.offer(this.currentConfig);
      }
    }
  }

  @Override
  public boolean isObserving(List<String> lanes) {
    if(currentConfig != null && currentConfig.getLaneToDataRuleMap() != null) {
      if(currentConfig.getRuleDefinition() != null &&
        currentConfig.getRuleDefinition().getMetricsAlertDefinitions() != null &&
        !currentConfig.getRuleDefinition().getMetricsAlertDefinitions().isEmpty()) {
        return true;
      }
      for (String lane : lanes) {
        if (currentConfig.getLaneToDataRuleMap().containsKey(lane)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void observe(Pipe pipe, Map<String, List<Record>> snapshot) {
    boolean offered;
    try {
      offered = observeRequests.offer(new ProductionObserveRequest(snapshot), 1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      offered = false;
    }
    if(!offered) {
      LOG.debug("Dropping batch as observer queue is full. " +
        "Please resize the observer queue or decrease the sampling percentage.");
      //raise alert to say that we dropped batch
      //reconfigure queue size or tune sampling %
    }
  }

  @Override
  public void setConfiguration(RulesConfigurationChangeRequest rulesConfigurationChangeRequest) {
    this.newConfig = rulesConfigurationChangeRequest;
  }

}
