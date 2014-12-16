/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LaneResolver {
  static final String STAGE_OUT       = "::s";
  static final String OBSERVER_OUT    = "::o";
  static final String MULTIPLEXER_OUT = "::m";
  static final String COMBINER_OUT    = "::c";
  private static final String ROUTING_SEPARATOR = "--";

  static List<String> getPostFixed(List<String> lanes, String postfix) {
    List<String> postFixed = new ArrayList<>(lanes.size());
    for (String lane : lanes) {
      postFixed.add(lane + postfix);
    }
    return ImmutableList.copyOf(postFixed);
  }

  static String createLane(String from, String to) {
    return from + ROUTING_SEPARATOR + to;
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
    List<String> list = new ArrayList<>();
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
    List<String> list = new ArrayList<>();
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
    List<String> names = new ArrayList<>(stages.length);
    for (StageRuntime stage : stages) {
      names.add(stage.getInfo().getInstanceName());
    }
    return Utils.format("LaneResolver[stages='{}']", names);
  }

}
