/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.runner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BatchMakerImpl implements BatchMaker {
  private final StagePipe stagePipe;
  private final String instanceName;
  private final Set<String> outputLanes;
  private final String singleOutputLane;
  private final Map<String, List<Record>> stageOutput;
  private final Map<String, List<Record>> stageOutputSnaphost;

  public BatchMakerImpl(StagePipe stagePipe, boolean keepSnapshot) {
    this.stagePipe = stagePipe;
    this.instanceName= stagePipe.getStage().getInfo().getInstanceName();
    outputLanes = ImmutableSet.copyOf(stagePipe.getStage().getConfiguration().getOutputLanes());
    singleOutputLane = (outputLanes.size() == 1) ? outputLanes.iterator().next() : null;
    stageOutput = new HashMap<String, List<Record>>();
    stageOutputSnaphost = (keepSnapshot) ? new HashMap<String, List<Record>>() : null;
    for (String outputLane : outputLanes) {
      stageOutput.put(outputLane, new ArrayList<Record>());
      if (stageOutputSnaphost != null) {
        stageOutputSnaphost.put(outputLane, new ArrayList<Record>());
      }
    }
  }

  public StagePipe getStagePipe() {
    return stagePipe;
  }

  @Override
  public Set<String> getLanes() {
    return outputLanes;
  }

  @Override
  public void addRecord(Record record, String... lanes) {
    Record outputRecord = new RecordImpl((RecordImpl) record);
    if (lanes.length == 0) {
      Preconditions.checkArgument(outputLanes.size() == 1, String.format(
          "No lane has been specified and the stage '%s' has multiple output lanes '%s'", instanceName, outputLanes));
      stageOutput.get(singleOutputLane).add(outputRecord);
    } else {
      for (String lane : lanes) {
        Preconditions.checkArgument(outputLanes.contains(lane), String.format(
            "Invalid output lane '%s' for stage '%s', available lanes '%s'", lane, instanceName, outputLanes));
        stageOutput.get(lane).add(outputRecord);
      }
    }
    if (stageOutputSnaphost != null) {
      if (lanes.length == 0) {
        stageOutputSnaphost.get(singleOutputLane).add(record);
      } else {
        for (String lane : lanes) {
          stageOutputSnaphost.get(lane).add(record);
        }
      }
    }
  }

  public Map<String, List<Record>> getStageOutput() {
    return stageOutput;
  }

  public Map<String, List<Record>> getStageOutputSnapshot() {
    return stageOutput;
  }

}
