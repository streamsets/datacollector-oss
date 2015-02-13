/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.prodmanager.Configuration;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.Pipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProductionObserver implements Observer {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionObserver.class);

  private final com.streamsets.pipeline.util.Configuration configuration;
  private BlockingQueue<Object> observeRequests;
  private volatile RulesConfigurationChangeRequest currentConfig;
  private volatile RulesConfigurationChangeRequest newConfig;

  public ProductionObserver(BlockingQueue<Object> observeRequests,
                            com.streamsets.pipeline.util.Configuration configuration) {
    this.observeRequests = observeRequests;
    this.configuration = configuration;
  }

  @Override
  public void reconfigure() {
    if(currentConfig != newConfig){
      this.currentConfig = this.newConfig;
      boolean offered = false;
      LOG.debug("Reconfiguring");
      while (!offered) {
        offered = observeRequests.offer(this.currentConfig);
      }
      LOG.debug("Reconfigured");
    }
  }

  @Override
  public boolean isObserving(List<String> lanes) {
    if(currentConfig != null && currentConfig.getLaneToDataRuleMap() != null) {
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
    Map<String, List<Record>> sampleSet = new HashMap<>();
    Map<String, Integer> laneToRecordsSizeMap = new HashMap<>();
    for(Map.Entry<String, List<Record>> entry : snapshot.entrySet()) {
      String lane = entry.getKey();
      List<Record> allRecords = entry.getValue();
      laneToRecordsSizeMap.put(lane, allRecords.size());
      List<DataRuleDefinition> dataRuleDefinitions = currentConfig.getLaneToDataRuleMap().get(lane);
      if(dataRuleDefinitions != null) {
        List<Record> sampleRecords = getSampleRecords(dataRuleDefinitions, allRecords);
        sampleSet.put(lane, sampleRecords);
      }
    }
    boolean offered;
    try {
      offered = observeRequests.offer(new DataRulesEvaluationRequest(sampleSet, laneToRecordsSizeMap),
        configuration.get(Configuration.MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_KEY,
        Configuration.MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_DEFAULT), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      offered = false;
    }
    if(!offered) {
      LOG.error("Dropping batch as observer queue is full. " +
        "Please resize the observer queue or decrease the sampling percentage.");
      //raise alert to say that we dropped batch
      //reconfigure queue size or tune sampling %
    }
  }

  @Override
  public void setConfiguration(RulesConfigurationChangeRequest rulesConfigurationChangeRequest) {
    this.newConfig = rulesConfigurationChangeRequest;
  }

  private List<Record> getSampleRecords(List<DataRuleDefinition> dataRuleDefinitions, List<Record> allRecords) {
    double percentage = 0;
    for(DataRuleDefinition dataRuleDefinition : dataRuleDefinitions) {
      if(dataRuleDefinition.getSamplingPercentage() > percentage) {
        percentage = dataRuleDefinition.getSamplingPercentage();
      }
    }
    Collections.shuffle(allRecords);
    double numberOfRecordsToSample = Math.floor(allRecords.size() * percentage / 100);
    List<Record> sampleRecords = new ArrayList<>((int)numberOfRecordsToSample);
    for(int i = 0; i < numberOfRecordsToSample; i++) {
      sampleRecords.add(((RecordImpl) allRecords.get(i)).clone());
    }
    return sampleRecords;
  }

}
