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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;

public class LaneResolver {
  static final int POSTFIX_LEN = 3;
  static final String SEPARATOR       = "::";
  static final String STAGE_OUT       = SEPARATOR + "s";
  static final String OBSERVER_OUT    = SEPARATOR + "o";
  static final String MULTIPLEXER_OUT = SEPARATOR + "m";
  static final String ROUTING_SEPARATOR = "--";

  private static void validatePostFix(String postFix) {
    Utils.checkState(postFix.length() == POSTFIX_LEN, Utils.format("'{}' invalid length, it should be '{}'", postFix,
                                                                   POSTFIX_LEN));
  }

  static {
    validatePostFix(STAGE_OUT);
    validatePostFix(OBSERVER_OUT);
    validatePostFix(MULTIPLEXER_OUT);
  }

  static List<String> getPostFixed(List<String> lanes, String postfix) {
    List<String> postFixed = new ArrayList<>(lanes.size());
    for (String lane : lanes) {
      postFixed.add(lane + postfix);
    }
    return ImmutableList.copyOf(postFixed);
  }

  static String createLane(String from, String to, String stageName) {
    return from + ROUTING_SEPARATOR + to + SEPARATOR + stageName;
  }

  private final List<StageRuntime> stages;

  public LaneResolver(List<StageRuntime> stages) {
    this.stages = stages;
  }

  public List<String> getStageInputLanes(int idx) {
    List<String> list = new ArrayList<>();
    for (String input : stages.get(idx).getConfiguration().getInputLanes()) {
      for (int i = 0; i < idx; i++) {
        for (String output : stages.get(i).getConfiguration().getOutputAndEventLanes()) {
          if (output.equals(input)) {
            list.add(createLane(
              output,
              stages.get(idx).getInfo().getInstanceName(),
              stages.get(i).getInfo().getInstanceName()
            ));
          }
        }
      }
    }
    return getPostFixed(list, MULTIPLEXER_OUT);
  }

  public List<String> getStageOutputLanes(int idx) {
    return getPostFixed(stages.get(idx).getConfiguration().getOutputLanes(), STAGE_OUT);
  }

  public List<String> getStageEventLanes(int idx) {
    return getPostFixed(stages.get(idx).getConfiguration().getEventLanes(), STAGE_OUT);
  }

  public List<String> getObserverInputLanes(int idx) {
    return getPostFixed(stages.get(idx).getConfiguration().getOutputAndEventLanes(), STAGE_OUT);
  }

  public List<String> getObserverOutputLanes(int idx) {
    return getPostFixed(stages.get(idx).getConfiguration().getOutputAndEventLanes(), OBSERVER_OUT);
  }

  public List<String> getMultiplexerInputLanes(int idx) {
    return getObserverOutputLanes(idx);
  }

  public List<String> getMultiplexerOutputLanes(int idx) {
    List<String> list = new ArrayList<>();
    for (String output : stages.get(idx).getConfiguration().getOutputAndEventLanes()) {
      for (int i = idx + 1; i < stages.size(); i++) {
        for (String input : stages.get(i).getConfiguration().getInputLanes()) {
          if (input.equals(output)) {
            list.add(createLane(
              output,
              stages.get(i).getInfo().getInstanceName(),
              stages.get(idx).getInfo().getInstanceName()
            ));
          }
        }
      }
    }
    return getPostFixed(list, MULTIPLEXER_OUT);
  }

  public static List<String> getMatchingOutputLanes(String source, List<String> output) {
    String prefix = source + ROUTING_SEPARATOR;
    List<String> list = new ArrayList<>();
    for (String lane : output) {
      if (lane.startsWith(prefix)) {
        list.add(lane);
      }
    }
    return list;
  }

  @Override
  public String toString() {
    List<String> names = new ArrayList<>(stages.size());
    for (StageRuntime stage : stages) {
      names.add(stage.getInfo().getInstanceName());
    }
    return Utils.format("LaneResolver[stages='{}']", names);
  }

  public static String getPostFixedLaneForObserver(String laneName) {
    return laneName + STAGE_OUT;
  }

  public static String removePostFixFromLane(String laneName) {
    return laneName.substring(0, laneName.length() - POSTFIX_LEN);
  }

}
