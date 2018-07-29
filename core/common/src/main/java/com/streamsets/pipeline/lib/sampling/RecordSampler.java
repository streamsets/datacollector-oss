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
package com.streamsets.pipeline.lib.sampling;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.Sampler;
import com.streamsets.pipeline.lib.util.SdcRecordConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Selects a percentage of records from the sample space.
 *
 */
public class RecordSampler implements Sampler {

  private final ProtoConfigurableEntity.Context stageContext;
  private List<Integer> populationSet;
  private final boolean isOrigin;
  private final int sampleSize;
  private int recordCounter;
  private Set<Integer> sampleSet;

  public RecordSampler(ProtoConfigurableEntity.Context stageContext, boolean isOrigin, int sampleSize, int populationSize) {
    this.stageContext = stageContext;
    this.isOrigin = isOrigin;
    this.sampleSize = sampleSize;

    if (sampleSize > 0) {
      sampleSet = new HashSet<>(sampleSize);
      populationSet = new ArrayList<>(populationSize);
      for (int i = 0; i < populationSize; i++) {
        populationSet.add(i);
      }
      chooseSampleFromPopulation();
    }
  }

  @Override
  public boolean sample(Record record) {
    boolean isSampled = false;
    if (isOrigin) {
      String sampled = record.getHeader().getAttribute(SdcRecordConstants.SDC_SAMPLED_RECORD);
      isSampled = (null != sampled && SdcRecordConstants.TRUE.equals(sampled));
      if (isSampled) {
        updateTimer(record);
      }
    } else if (sampleSize > 0){
      if (sampleSet.remove(recordCounter)) {
        updateRecordHeader(record);
        isSampled = true;
      }
      recordCounter++;
      if (sampleSet.isEmpty()) {
        recordCounter = 0;
        chooseSampleFromPopulation();
      }
    }
    return isSampled;
  }

  private void chooseSampleFromPopulation() {
    if (sampleSize > 0) {
      Collections.shuffle(populationSet);
      sampleSet.clear();
      sampleSet.addAll(populationSet.subList(0, sampleSize));
    }
  }

  private void updateTimer(Record record) {
    long sampledTimeAtDest = Long.parseLong(record.getHeader().getAttribute(SdcRecordConstants.SDC_SAMPLED_TIME));
    long currentTime = System.currentTimeMillis();
    long timeSpentInOrigin = currentTime - sampledTimeAtDest;
    Timer timer = stageContext.getTimer(SdcRecordConstants.EXTERNAL_SYSTEM_LATENCY);
    if (null == timer) {
      timer = stageContext.createTimer(SdcRecordConstants.EXTERNAL_SYSTEM_LATENCY);
    }
    timer.update(timeSpentInOrigin, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  void updateRecordHeader(Record record) {
    record.getHeader().setAttribute(SdcRecordConstants.SDC_SAMPLED_RECORD, SdcRecordConstants.TRUE);
    record.getHeader().setAttribute(SdcRecordConstants.SDC_SAMPLED_TIME, String.valueOf(System.currentTimeMillis()));
  }
}
