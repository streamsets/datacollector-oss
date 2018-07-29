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
package com.streamsets.datacollector.runner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BatchMakerImpl implements BatchMaker {

  private static final Logger LOG = LoggerFactory.getLogger(BatchMakerImpl.class);

  private final StagePipe stagePipe;
  private final String instanceName;
  private final List<String> outputLanes;
  private final String singleOutputLane;
  private final Map<String, List<Record>> stageOutput;
  private final Map<String, List<Record>> stageOutputSnapshot;
  private int recordAllowance;
  private int size;
  private boolean recordByRef;
  private Optional<RateLimiter> rateLimiterOptional = Optional.absent();

  public BatchMakerImpl(StagePipe stagePipe, boolean keepSnapshot) {
    this(stagePipe, keepSnapshot, Integer.MAX_VALUE);
  }

  public BatchMakerImpl(StagePipe stagePipe, boolean keepSnapshot, int recordAllowance) {
    this.stagePipe = stagePipe;
    this.instanceName= stagePipe.getStage().getInfo().getInstanceName();
    outputLanes = ImmutableList.copyOf(stagePipe.getStage().getConfiguration().getOutputLanes());
    singleOutputLane = (outputLanes.size() == 1) ? outputLanes.iterator().next() : null;
    stageOutput = new HashMap<>();
    stageOutputSnapshot = (keepSnapshot) ? new HashMap<String, List<Record>>() : null;
    for (String outputLane : outputLanes) {
      stageOutput.put(outputLane, new ArrayList<Record>());
      if (stageOutputSnapshot != null) {
        stageOutputSnapshot.put(outputLane, new ArrayList<Record>());
      }
    }
    this.recordAllowance = recordAllowance;
    // if the stage is annotated as recordsByRef it means it does not reuse the records it creates, thus
    // we can skip one copy here (just here though), except if we are in preview
    recordByRef = !stagePipe.getStage().getContext().isPreview() &&
                  stagePipe.getStage().getDefinition().getRecordsByRef();
  }

  boolean isRecordByRef() {
    return recordByRef;
  }

  public StagePipe getStagePipe() {
    return stagePipe;
  }

  @Override
  public List<String> getLanes() {
    return outputLanes;
  }

  @VisibleForTesting
  RecordImpl getRecordForBatchMaker(Record record) {
    // in the constructor we figured out if we can do recordByRef or not
    return (recordByRef) ? (RecordImpl) record: ((RecordImpl) record).clone();
  }

  @Override
  public void addRecord(Record record, String... lanes) {

    if (recordAllowance-- == 0) {
      //Some origins like "Kafka source" translate one message into multiple records [think JSON multiple objects mode]
      //the number of records may tip over the max batch size [both in preview and run].
      //Allow this. Max batch size is more of a guideline.
      LOG.warn("The maximum number of records per batch in the origin has been exceeded.");
    }
    Preconditions.checkNotNull(record, "record cannot be null");

    RecordImpl recordCopy = getRecordForBatchMaker(record);
    recordCopy.addStageToStagePath(instanceName);
    recordCopy.createTrackingId();

    if (recordCopy.isInitialRecord()) {
      RecordImpl recordSource = recordCopy.clone();
      recordCopy.getHeader().setSourceRecord(recordSource);
      recordCopy.setInitialRecord(false);
    }

    if (getStagePipe().getStage().getDefinition().getType() == StageType.SOURCE) {
      // Now slow down until we can actually add the record.
      if (rateLimiterOptional.isPresent()) {
        rateLimiterOptional.get().acquire();
      }
    }

    if (lanes.length == 0) {
      Preconditions.checkArgument(outputLanes.size() == 1, Utils.formatL(
          "No stream has been specified and the stage '{}' has multiple output streams '{}'", instanceName, outputLanes));
      stageOutput.get(singleOutputLane).add(recordCopy);
    } else {
      if (lanes.length > 1) {
        Set<String> laneSet = ImmutableSet.copyOf(lanes);
        Preconditions.checkArgument(laneSet.size() == lanes.length, Utils.formatL(
            "Specified streams cannot have duplicates '{}'", laneSet));
      }
      for (String lane : lanes) {
        Preconditions.checkArgument(outputLanes.contains(lane), Utils.formatL(
            "Invalid output stream '{}' for stage '{}', available streams '{}'", lane, instanceName, outputLanes));
        stageOutput.get(lane).add(recordCopy);
      }
    }
    if (stageOutputSnapshot != null) {
      recordCopy = recordCopy.clone();
      if (lanes.length == 0) {
        stageOutputSnapshot.get(singleOutputLane).add(recordCopy);
      } else {
        for (String lane : lanes) {
          stageOutputSnapshot.get(lane).add(recordCopy);
        }
      }
    }
    size++;
  }

  public Map<String, List<Record>> getStageOutput() {
    return stageOutput;
  }

  public Map<String, List<Record>> getStageOutputSnapshot() {
    return stageOutputSnapshot;
  }

  public int getSize() {
    return size;
  }

  public int getSize(String lane) {
    return stageOutput.get(lane).size();
  }

  public void setRateLimiter(@Nullable RateLimiter rateLimiter) {
    rateLimiterOptional = Optional.fromNullable(rateLimiter);
  }

  @Override
  public String toString() {
    return Utils.format("BatchMakerImpl[instance='{}' lanes='{}' size='{}' keepsSnapshot='{}']", instanceName,
                        getLanes(), getSize(), stageOutputSnapshot != null);
  }
}
