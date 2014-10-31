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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LaneResolver {
  static final String STAGE_OUT       = "::s";
  static final String OBSERVER_OUT    = "::o";
  static final String MULTIPLEXER_OUT = "::m";
  static final String COMBINER_OUT    = "::c";

  static List<String> getPostFixed(List<String> lanes, String postfix) {
    List<String> postFixed = new ArrayList<String>(lanes.size());
    for (String lane : lanes) {
      postFixed.add(lane + postfix);
    }
    return ImmutableList.copyOf(postFixed);
  }

  static String createLane(String from, String to) {
    return from + "--" + to;
  }

  private final StageRuntime[] stages;

  public LaneResolver(StageRuntime[] stages) {
    this.stages = stages;
  }

  public List<String> getStageInputLanes(int idx) {
    return getCombinerOutputLanes(idx);
  }

  public List<String> getStageOutputLanes(int idx) {
    return getPostFixed(stages[idx].getConfiguration().getOutputLanes(), STAGE_OUT);
  }

  public List<String> getObserverInputLanes(int idx) {
    return getStageOutputLanes(idx);
  }

  public List<String> getObserverOutputLanes(int idx) {
    return getPostFixed(stages[idx].getConfiguration().getOutputLanes(), OBSERVER_OUT);
  }

  public List<String> getMultiplexerInputLanes(int idx) {
    return getObserverOutputLanes(idx);
  }

  public List<String> getMultiplexerOutputLanes(int idx) {
    List<String> list = new ArrayList<String>();
    for (String output : stages[idx].getConfiguration().getOutputLanes()) {
      for (int i = idx + 1; i < stages.length; i++) {
        for (String input : stages[i].getConfiguration().getInputLanes()) {
          if (input.equals(output)) {
            list.add(createLane(output, stages[i].getInfo().getInstanceName()));
          }
        }
      }
    }
    return getPostFixed(list, MULTIPLEXER_OUT);
  }

  public List<String> getCombinerInputLanes(int idx) {
    List<String> list = new ArrayList<String>();
    for (String input : stages[idx].getConfiguration().getInputLanes()) {
      for (int i = 0; i < idx; i++) {
        for (String output : stages[i].getConfiguration().getOutputLanes()) {
          if (output.equals(input)) {
            list.add(createLane(output, stages[idx].getInfo().getInstanceName()));
          }
        }
      }
    }
    return getPostFixed(list, MULTIPLEXER_OUT);
  }

  @SuppressWarnings("unchecked")
  public List<String> getCombinerOutputLanes(int idx) {
    boolean noInput = stages[idx].getConfiguration().getInputLanes().isEmpty();
    return (noInput) ? Collections.EMPTY_LIST : getPostFixed(ImmutableList.of(stages[idx].getInfo().getInstanceName()),
                                                             COMBINER_OUT);
  }

}
