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
package com.streamsets.pipeline.stage.processor.selector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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
    Processor selector = new SelectorProcessor(lanePredicates);
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class, selector)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("SELECTOR_00"));

    lanePredicates = createLanePredicates("a","${true}", "b", "${true}");
    selector = new SelectorProcessor(lanePredicates);
    runner = new ProcessorRunner.Builder(SelectorDProcessor.class, selector)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("SELECTOR_01"));

    lanePredicates = createLanePredicates("a","${true}", "b", "${true}");

    selector = new SelectorProcessor(lanePredicates);
    runner = new ProcessorRunner.Builder(SelectorDProcessor.class, selector)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("SELECTOR_07"));

    lanePredicates = createLanePredicates("x","x${true}", "b", "${x + 'x'}", "c", "default");

    Map<String, Object> constants = new HashMap<>();
    constants.put("a-x", 1);
    selector = new SelectorProcessor(lanePredicates);
    runner = new ProcessorRunner.Builder(SelectorDProcessor.class, selector)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .addOutputLane("b")
        .addOutputLane("c")
        .addConstants(constants)
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(2, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("SELECTOR_02"));
    Assert.assertTrue(issues.get(1).toString().contains("SELECTOR_08"));
  }

  @Test
  public void testInitLanePredicates() throws Exception {
    Map<String, Object> constants = new HashMap<>();
    constants.put("x", "false");
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("lanePredicates", createLanePredicates("a", "${x}", "b", "default"))
        .addConstants(constants)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();
    runner.runInit();
  }

  @Test
  public void testInitLanePredicatesFragment() throws Exception {
    Map<String, Object> constants = new HashMap<>();
    constants.put("x", "false");
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("lanePredicates", createLanePredicates("a", "${x}", "b", "default"))
        .addConstants(constants)
        .addOutputLane("somefragment-a")
        .addOutputLane("somefragment-b")
        .build();
    runner.runInit();
  }

  @Test
  public void testInitLanePredicatesWithListMapConstants() throws Exception {
    Map<String, Object> constants = new HashMap<>();
    constants.put("x", "false");
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("lanePredicates", createLanePredicates("a", "${x}", "b", "default"))
        .addConstants(constants)
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
  public void testSelectWithTime() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SelectorDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("lanePredicates",
            createLanePredicates(
            "a", "${record:value('') > time:extractDateFromString('2015-04-23', 'yyyy-MM-dd')}",
            "b", "default"))
        .addOutputLane("a")
        .addOutputLane("b")
        .build();

    runner.runInit();
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      // force tests use GMT, so they work anywhere in the world with hard coded dates.
      sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

      Date dt0 = sdf.parse("2014-04-22");
      Record r0 = RecordCreator.create();
      r0.set(Field.createDate(dt0));

      Date dt1 = sdf.parse("2016-05-29");
      Record r1 = RecordCreator.create();
      r1.set(Field.createDate(dt1));

      List<Record> input = ImmutableList.of(r0, r1);
      StageRunner.Output output = runner.runProcess(input);

      Assert.assertEquals(ImmutableSet.of("a", "b"), output.getRecords().keySet());

      Assert.assertEquals(1, output.getRecords().get("a").size());
      String ans = sdf.format(output.getRecords().get("a").get(0).get().getValue());
      Assert.assertEquals("2016-05-29", ans);

      Assert.assertEquals(1, output.getRecords().get("b").size());
      ans = sdf.format(output.getRecords().get("b").get(0).get().getValue());
      Assert.assertEquals("2014-04-22", ans);

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
