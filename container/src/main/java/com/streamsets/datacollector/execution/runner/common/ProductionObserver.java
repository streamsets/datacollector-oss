/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.runner.common;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.Pipe;
import com.streamsets.datacollector.runner.production.DataRulesEvaluationRequest;
import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.Record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProductionObserver implements Observer {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionObserver.class);

  private final com.streamsets.datacollector.util.Configuration configuration;
  private BlockingQueue<Object> observeRequests;
  private final MetricsObserverRunner metricsObserverRunner;

  private volatile RulesConfigurationChangeRequest currentConfig;
  private volatile RulesConfigurationChangeRequest newConfig;

   /*A lane has multiple rules with different sampling percentages.
    Pick the one with highest sampling %, say n and generate n integers between 0 and 99.
    All other rules for that lane pick a subset of numbers from that generated list based on their sampling %.
    Maintain a record counter per lane that counts the records that flows through it.
    If the counter matches a generated number, that record is picked uo, cloned and sent for evaluation.
    This map holds the n generated numbers for the rule with max sampling % */
  private final ThreadLocal<Map<String, Set<Integer>>> laneToMaxRecordIndexMap;
  /*This map holds the generated numbers for each rule, which is selected from the n generated numbers for the rule
  with max sampling %.*/
  private final ThreadLocal<Map<String, Set<Integer>>> ruleIdToRecordIndexMap;
  /*This map holds the count of records that have flown though that lane. The counter is reset when it reaches 100*/
  private final ThreadLocal<Map<String, Integer>> laneToRecordCounterMap;
  /*Contains integers between 0 and 99 and n random numbers are selected by shuffling this list and picking the first
  n*/
  private final ThreadLocal<List<Integer>> randomNumberSampleSpace;

  @Inject
  public ProductionObserver(Configuration configuration, MetricsObserverRunner metricsObserverRunner) {
    this.configuration = configuration;
    this.metricsObserverRunner = metricsObserverRunner;
    this.laneToMaxRecordIndexMap = ThreadLocal.withInitial(HashMap::new);
    this.ruleIdToRecordIndexMap = ThreadLocal.withInitial(HashMap::new);
    this.laneToRecordCounterMap = ThreadLocal.withInitial(HashMap::new);
    this.randomNumberSampleSpace = ThreadLocal.withInitial(() -> {
      List<Integer> randomNumberSampleSpace = new ArrayList<>(100);
      for(int i = 0; i < 100; i++) {
        randomNumberSampleSpace.add(i);
      }

      return randomNumberSampleSpace;
    });
  }

  public void setObserveRequests(BlockingQueue<Object> observeRequests) {
    this.observeRequests = observeRequests;
  }

  @Override
  public void setPipelineStartTime(long pipelineStartTime) {
    metricsObserverRunner.setPipelineStartTime(pipelineStartTime);
  }

  @Override
  public void reconfigure() {
    if(currentConfig != newConfig){
      this.currentConfig = this.newConfig;
      boolean offered = false;
      LOG.debug("Reconfiguring");
      //update the changes rule configuration in the metrics observer
      if(metricsObserverRunner != null) {
        metricsObserverRunner.setRulesConfigurationChangeRequest(this.currentConfig);
      }
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
    Map<String, Map<String, List<Record>>> laneToRecordsMap = new HashMap<>();
    Map<String, List<Record>> ruleIdToRecordsMap = new HashMap<>();
    Map<String, Integer> laneToRecordsSizeMap = new HashMap<>();
    for(Map.Entry<String, List<Record>> entry : snapshot.entrySet()) {
      String lane = entry.getKey();
      List<Record> allRecords = entry.getValue();
      laneToRecordsSizeMap.put(lane, allRecords.size());
      List<DataRuleDefinition> dataRuleDefinitions = currentConfig.getLaneToDataRuleMap().get(lane);
      if(dataRuleDefinitions != null) {
        Map<String, List<Record>> sampleRecords = getSampleRecords(dataRuleDefinitions, allRecords, lane);
        for(Map.Entry<String, List<Record>> e : sampleRecords.entrySet()) {
          ruleIdToRecordsMap.put(e.getKey(), e.getValue());
        }
      }
      laneToRecordsMap.put(lane, ruleIdToRecordsMap);
    }
    boolean offered;
    try {
      offered = observeRequests.offer(new DataRulesEvaluationRequest(laneToRecordsMap, laneToRecordsSizeMap),
        configuration.get(Constants.MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_KEY,
        Constants.MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_DEFAULT), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      offered = false;
    }
    if(!offered) {
      LOG.error("Dropping DataRules Evaluation Request as observer queue is full. " +
        "Please resize the observer queue or decrease the sampling percentage.");
      //reconfigure queue size or tune sampling %
    }
  }

  @Override
  public void setConfiguration(RulesConfigurationChangeRequest rulesConfigurationChangeRequest) {
    this.newConfig = rulesConfigurationChangeRequest;
  }

  @VisibleForTesting
  Map<String, List<Record>> getSampleRecords(List<DataRuleDefinition> dataRuleDefinitions,
                                                     List<Record> allRecords, String lane) {

    // Tucu's Algorithm for sampling
    /*
        1* pick the highest sampling percentage, X for that lane
        2* generate X different random number between 0 and 99
        4* send a counter reset event to the queue
        5* start a record counter as records flow through the observer
        6* when counter equals one of the X random numbers, that is sampling record
        7* when record count hits 100 go back to #2
    */

    //Implementation notes:
    /*

      Algorithm implementation:
      -------------------------
      To start with, all maps and counters are empty.

      Say this lane has 2 rules, rule 1 and rule 2 with sampling % N and M respectively, N > M.

      In the first iteration, the rule with the max sampling %, for that lane is selected, which happens to be N in our
      case, and N unique integers between 0 and 99 are generated.
      Look at "getRecordsToPickUp(List<DataRuleDefinition> dataRuleDefinitions)" method for the generation algorithm.

      rule 2 will pick up M numbers from the set of N numbers generated for rule1.
      This way we clone only N% of records and both rules share the same cloned records.

      rule1 evaluates all sampled records and rule2 will evaluate its share of records from the same list.

      Picking up new configuration:
      -----------------------------

      When the record counter for that lane hits 100, it is reset to 0.
      At that moment all maps and counters are cleared so that in the next iteration everything is computed fresh again.

      This is the moment when any changes to sampling % of data rule definitions are picked up.

      very Low throughput scenario:
      -----------------------------

      Look at the following unit test that tries to simulate the very Low throughput scenario:
      "com.streamsets.pipeline.runner.production.TestProductionObserver.testGetSampledRecordsLowThroughput"
    */

    //record counter for this lane, reset when the count reaches 100
    int recordCounter = 0;
    if(laneToRecordCounterMap.get().containsKey(lane)) {
      recordCounter = laneToRecordCounterMap.get().get(lane);
    } else {
      laneToRecordCounterMap.get().put(lane, recordCounter);
    }
    Map<String, List<Record>> sampledRecordsMap = new HashMap<>();

    //Get the max percentage records to be sampled for this lane.
    //Generates a set n random integers, where n is max percentage for this lane [say n = 50] between 0 and 99
    //The set of integers generated is cached on a per lane basis.
    Set<Integer> recordIndexToPickup;
    if(laneToMaxRecordIndexMap.get().containsKey(lane)) {
      recordIndexToPickup = laneToMaxRecordIndexMap.get().get(lane);
    } else {
      recordIndexToPickup = getRecordsToPickUp(dataRuleDefinitions);
      laneToMaxRecordIndexMap.get().put(lane, recordIndexToPickup);
    }

    //Go over all records for this lane and determine if it needs to be cloned.
    for(Record record : allRecords) {
      if(recordIndexToPickup.contains(recordCounter)) {
        //needs to be cloned
        Record recordClone = ((RecordImpl) record).clone();
        for(DataRuleDefinition d : dataRuleDefinitions) {
          //for every rule in this lane, check the integers generated for it to see if this record needs to be
          //sampled
          Set<Integer> recordsToPickup;
          if (ruleIdToRecordIndexMap.get().containsKey(d.getId())) {
            recordsToPickup = ruleIdToRecordIndexMap.get().get(d.getId());
          } else {
            //the integers generated to pick up records for sampling is shared between all rules for that lane.
            //the rule with least percentage contains records which are subset of rule with max percentage.
            //this is to minimize cloning of records.
            recordsToPickup = getRecordsToPickUp(recordIndexToPickup, (int)d.getSamplingPercentage());
            ruleIdToRecordIndexMap.get().put(d.getId(), recordsToPickup);
          }
          if (recordsToPickup.contains(recordCounter)) {
            //this record must be picked up by this rule
            List<Record> sampledRecords = sampledRecordsMap.get(d.getId());
            if (sampledRecords == null) {
              sampledRecords = new ArrayList<>();
              sampledRecordsMap.put(d.getId(), sampledRecords);
            }
            sampledRecords.add(recordClone);
          }
        }
      }
      recordCounter++;
      if(recordCounter == 100) {
        //clear all records to pick up.
        //It will be generated the next time the sampling is attempted
        //It will also pick up any changes done to sampling percentages of rules in the next iteration.
        recordCounter = 0;
        for(DataRuleDefinition d : dataRuleDefinitions) {
          ruleIdToRecordIndexMap.get().remove(d.getId());
        }
        laneToMaxRecordIndexMap.get().remove(lane);
      }
    }
    laneToRecordCounterMap.get().put(lane, recordCounter);
    return sampledRecordsMap;
  }

  private Set<Integer> getRecordsToPickUp(List<DataRuleDefinition> dataRuleDefinitions) {
    //Max percentage
    double percentage = 0;
    for(DataRuleDefinition dataRuleDefinition : dataRuleDefinitions) {
      if (dataRuleDefinition.getSamplingPercentage() > percentage) {
        percentage = dataRuleDefinition.getSamplingPercentage();
      }
    }
    Set<Integer> recordsToPickup = new HashSet<>();
    Collections.shuffle(randomNumberSampleSpace.get());
    recordsToPickup.addAll(randomNumberSampleSpace.get().subList(0, (int) percentage));
    return recordsToPickup;
  }

  private Set<Integer> getRecordsToPickUp(Set<Integer> recordIndexToPickup, int samplePercentage) {
    Set<Integer> recordsToPickup = new HashSet<>();
    Iterator<Integer> iterator = recordIndexToPickup.iterator();
    int counter = 0;
    while(iterator.hasNext() && counter < samplePercentage) {
      recordsToPickup.add(iterator.next());
      counter++;
    }
    return recordsToPickup;
  }

}
