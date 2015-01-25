/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.observerstore.ObserverStore;
import com.streamsets.pipeline.runner.LaneResolver;
import com.streamsets.pipeline.util.ObserverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordSampler {

  private static final Logger LOG = LoggerFactory.getLogger(RecordSampler.class);

  private static final int MAX_SAMPLED_RECORDS_PER_SAMPLE_DEF = 100;

  private final SamplingDefinition samplingDefinition;
  private final ObserverStore observerStore;
  private final ELEvaluator.Variables variables;
  private final ELEvaluator elEvaluator;
  private final String pipelineName;
  private final String rev;

  public RecordSampler(String pipelineName, String rev, SamplingDefinition samplingDefinition,
                       ObserverStore observerStore, ELEvaluator.Variables variables, ELEvaluator elEvaluator) {
    this.pipelineName = pipelineName;
    this.rev = rev;
    this.samplingDefinition = samplingDefinition;
    this.observerStore = observerStore;
    this.variables = variables;
    this.elEvaluator = elEvaluator;
  }

  public void sample(Map<String, List<Record>> snapshot,Map<String, EvictingQueue<Record>> sampleIdToRecordsMap) {

    if(samplingDefinition.isEnabled()) {
      String lane = samplingDefinition.getLane();
      String predicate = samplingDefinition.getPredicate();
      List<Record> records = snapshot.get(LaneResolver.getPostFixedLaneForObserver(lane));
      double samplingPercentage = Double.valueOf(samplingDefinition.getSamplingPercentage());
      double numberOfRecordsToSample = Math.floor(records.size() * samplingPercentage/100);
      Collections.shuffle(records);
      List<Record> samplingRecords = records.subList(0, (int) numberOfRecordsToSample);
      List<Record> matchingRecords = new ArrayList<>();
      if(predicate == null || predicate.isEmpty()) {
        matchingRecords.addAll(samplingRecords);
      } else {
        for (Record r : samplingRecords) {
          boolean success = false;
          try {
            success = AlertsUtil.evaluateRecord(r, predicate, variables, elEvaluator);
          } catch (ObserverException e) {
            //A faulty condition should not take down rest of the alerts with it.
            //Log and it and continue for now
            LOG.error("Error processing sampling definition '{}', reason: {}", samplingDefinition.getId(),
              e.getMessage());
          }
          if (success) {
            matchingRecords.add(r);
          }
        }
      }
      if(!matchingRecords.isEmpty()) {
        Map<String, List<Record>> sampleIdToRecords = new HashMap<>();
        sampleIdToRecords.put(samplingDefinition.getId(), matchingRecords);
        //store sampleIdToRecords to file
        observerStore.storeSampledRecords(pipelineName, rev, sampleIdToRecords);
        //retain sampleIdToRecords in memory
        EvictingQueue<Record> sampledRecords = sampleIdToRecordsMap.get(samplingDefinition.getId());
        if (sampledRecords == null) {
          sampledRecords = EvictingQueue.create(MAX_SAMPLED_RECORDS_PER_SAMPLE_DEF);
          sampleIdToRecordsMap.put(samplingDefinition.getId(), sampledRecords);
        }
        sampledRecords.addAll(sampleIdToRecords.get(samplingDefinition.getId()));
      }
    }
  }
}
