/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.selector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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

  @Test
  @SuppressWarnings("unchecked")
  public void testValidation() throws Exception {
    List<Map<String, String>> lanePredicates = createLanePredicates();
    Map<String, ?> constants = Collections.EMPTY_MAP;
    Processor selector = new SelectorProcessor(lanePredicates, constants);
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class, selector)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("SELECTOR_00"));

    lanePredicates = createLanePredicates("a","${true}", "b", "${true}");
    constants = Collections.EMPTY_MAP;
    selector = new SelectorProcessor(lanePredicates, constants);
    runner = new ProcessorRunner.Builder(SelectorDProcessor.class, selector)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("SELECTOR_01"));

    lanePredicates = createLanePredicates("a","${true}", "b", "${true}");
    constants = Collections.EMPTY_MAP;
    selector = new SelectorProcessor(lanePredicates, constants);
    runner = new ProcessorRunner.Builder(SelectorDProcessor.class, selector)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("SELECTOR_07"));

    lanePredicates = createLanePredicates("x","x${true}", "b", "${x + 'x'}", "c", "default");
    constants = ImmutableMap.of("a-x", 1);
    selector = new SelectorProcessor(lanePredicates, constants);
    runner = new ProcessorRunner.Builder(SelectorDProcessor.class, selector)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .addOutputLane("b")
        .addOutputLane("c")
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(4, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("SELECTOR_02"));
    Assert.assertTrue(issues.get(1).toString().contains("SELECTOR_04"));
    Assert.assertTrue(issues.get(2).toString().contains("SELECTOR_08"));
    Assert.assertTrue(issues.get(3).toString().contains("SELECTOR_03"));

  }

  @Test
  public void testInitLanePredicates() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("lanePredicates", createLanePredicates("a", "${x}", "b", "default"))
        .addConfiguration("constants", ImmutableMap.of("x", "false"))
        .addOutputLane("a")
        .addOutputLane("b")
        .build();
    runner.runInit();
  }

  @Test
  public void testInitLanePredicatesWithListMapConstants() throws Exception {
    List<Map> constant = ImmutableList.of((Map)ImmutableMap.of("key", "x", "value", "false"));
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("lanePredicates", createLanePredicates("a", "${x}", "b", "default"))
        .addConfiguration("constants", constant)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();
    runner.runInit();
  }

  @Test
  public void testSelect() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("lanePredicates", createLanePredicates("a", "${record:value('') == 1}",
                                                                 "b", "${record:value('') == 2}",
                                                                 "c", "default"))
        .addConfiguration("constants", null)
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
  public void testSelectError() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("lanePredicates", createLanePredicates("a", "${record:value('') == 1}",
                                                                 "b", "${record:value('') == 2}",
                                                                 "c", "default"))
        .addConfiguration("constants", null)
        .addOutputLane("a")
        .addOutputLane("b")
        .addOutputLane("c")
        .build();

    runner.runInit();
    try {
      Record r0 = RecordCreator.create();
      r0.set(Field.createDate(new Date()));
      Record r1 = RecordCreator.create();
      r1.set(Field.create(1));
      List<Record> input = ImmutableList.of(r0, r1);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(ImmutableSet.of("a", "b", "c"), output.getRecords().keySet());
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertEquals(0, output.getRecords().get("b").size());
      Assert.assertEquals(0, output.getRecords().get("c").size());
      Assert.assertEquals(1, output.getRecords().get("a").get(0).get().getValueAsInteger());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals(r0.getHeader().getSourceId(), runner.getErrorRecords().get(0).getHeader().getSourceId());
    } finally {
      runner.runDestroy();
    }
  }

}
