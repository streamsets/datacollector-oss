/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.selector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestSelectorProcessor {

  private List<Map<String, String>> createLanePredicates(String ... args) {
    List<Map<String, String>> lanePredicates = new ArrayList<>();
    if (args.length % 2 != 0) {
      Assert.fail("LanPredicates must come in pairs");
    }
    for (int i = 0; i < args.length; i = i + 2) {
      Map<String, String> map = new LinkedHashMap<>();
      map.put("outputLane", args[i]);
      map.put("predicate", args[i + 1]);
      lanePredicates.add(map);
    }
    return lanePredicates;
  }

  @Test(expected = IllegalStateException.class)
  public void testInitNullLanePredicates() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", null)
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .build();
    runner.runInit();
  }

  @Test(expected = IllegalStateException.class)
  public void testInitZeroLanePredicates() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates())
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .build();
    runner.runInit();
  }

  @Test(expected = IllegalStateException.class)
  public void testInitLanePredicatesNotMatchingLanes() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "true"))
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .build();
    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testInitLanePredicatesMoreOutputLanes() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "true"))
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();
    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testInitLanePredicatesInvalidPredicate() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "x"))
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .addOutputLane("a")
        .build();
    runner.runInit();
  }

  @Test
  public void testInitLanePredicates() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "x"))
        .addConfiguration("constants", ImmutableMap.of("x", "false"))
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .addOutputLane("a")
        .build();
    runner.runInit();
  }

  @Test
  public void testInitLanePredicatesWithListMapConstants() throws Exception {
    List<Map> constant = ImmutableList.of((Map)ImmutableMap.of("key", "x", "value", "false"));
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "x"))
        .addConfiguration("constants", constant)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .addOutputLane("a")
        .build();
    runner.runInit();
  }

  @Test
  public void testSelectWithDefault() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "record:value('') == 1",
                                                                 "b", "record:value('') == 2",
                                                                 "c", "default"))
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .addOutputLane("a")
        .addOutputLane("b")
        .addOutputLane("c")
        .build();

    runner.runInit();
    try {
      Record r0 = RecordCreator.create();
      r0.set(Field.create(0));
      Record r1 = RecordCreator.create();
      r1.set(Field.create(1));
      Record r2 = RecordCreator.create();
      r2.set(Field.create(2));
      Record r3 = RecordCreator.create();
      r3.set(Field.create(3));
      List<Record> input = ImmutableList.of(r0, r1, r2, r3);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(ImmutableSet.of("a", "b", "c"), output.getRecords().keySet());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertEquals(1, output.getRecords().get("a").get(0).get().getValueAsInteger());
      Assert.assertEquals(1, output.getRecords().get("b").size());
      Assert.assertEquals(2, output.getRecords().get("b").get(0).get().getValueAsInteger());
      Assert.assertEquals(2, output.getRecords().get("c").size());
      Assert.assertEquals(0, output.getRecords().get("c").get(0).get().getValueAsInteger());
      Assert.assertEquals(3, output.getRecords().get("c").get(1).get().getValueAsInteger());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSelectWithoutDefaultDropping() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "record:value('') == 1",
                                                                 "b", "record:value('') == 2"))
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();

    runner.runInit();
    try {
      Record r0 = RecordCreator.create();
      r0.set(Field.create(0));
      Record r1 = RecordCreator.create();
      r1.set(Field.create(1));
      Record r2 = RecordCreator.create();
      r2.set(Field.create(2));
      Record r3 = RecordCreator.create();
      r3.set(Field.create(3));
      List<Record> input = ImmutableList.of(r0, r1, r2, r3);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(ImmutableSet.of("a", "b"), output.getRecords().keySet());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertEquals(1, output.getRecords().get("a").get(0).get().getValueAsInteger());
      Assert.assertEquals(1, output.getRecords().get("b").size());
      Assert.assertEquals(2, output.getRecords().get("b").get(0).get().getValueAsInteger());
      Assert.assertTrue(runner.getErrors().isEmpty());
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSelectWithoutDefaultToError() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "record:value('') == 1",
                                                                 "b", "record:value('') == 2"))
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.RECORD_TO_ERROR)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();

    runner.runInit();
    try {
      Record r0 = RecordCreator.create();
      r0.set(Field.create(0));
      Record r1 = RecordCreator.create();
      r1.set(Field.create(1));
      Record r2 = RecordCreator.create();
      r2.set(Field.create(2));
      Record r3 = RecordCreator.create();
      r3.set(Field.create(3));
      List<Record> input = ImmutableList.of(r0, r1, r2, r3);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(ImmutableSet.of("a", "b"), output.getRecords().keySet());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertEquals(1, output.getRecords().get("a").get(0).get().getValueAsInteger());
      Assert.assertEquals(1, output.getRecords().get("b").size());
      Assert.assertEquals(2, output.getRecords().get("b").get(0).get().getValueAsInteger());
      Assert.assertTrue(runner.getErrors().isEmpty());
      Assert.assertEquals(2, runner.getErrorRecords().size());
      Assert.assertEquals(0, runner.getErrorRecords().get(0).get().getValueAsInteger());
      Assert.assertEquals(3, runner.getErrorRecords().get(1).get().getValueAsInteger());
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testSelectWithoutDefaultFailPipeline() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorProcessor.class)
        .addConfiguration("lanePredicates", createLanePredicates("a", "record:value('') == 1",
                                                                 "b", "record:value('') == 2"))
        .addConfiguration("constants", null)
        .addConfiguration("onNoPredicateMatch", OnNoPredicateMatch.FAIL_PIPELINE)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();

    runner.runInit();
    try {
      Record r0 = RecordCreator.create();
      r0.set(Field.create(0));
      Record r1 = RecordCreator.create();
      r1.set(Field.create(1));
      Record r2 = RecordCreator.create();
      r2.set(Field.create(2));
      Record r3 = RecordCreator.create();
      r3.set(Field.create(3));
      List<Record> input = ImmutableList.of(r0, r1, r2, r3);
      runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }
  }

}
