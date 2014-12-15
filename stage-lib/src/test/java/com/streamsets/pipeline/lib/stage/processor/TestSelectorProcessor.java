/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.stage.processor.selector.OnNoPredicateMatch;
import com.streamsets.pipeline.lib.stage.processor.selector.SelectorProcessor;
import com.streamsets.pipeline.sdk.testharness.ProcessorRunner;
import com.streamsets.pipeline.sdk.testharness.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSelectorProcessor {

  @Test(expected = StageException.class)
  public void testInitNullLanePredicates() throws Exception {
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(null).addProcessor(new SelectorProcessor())
                                           .configure("lanePredicates", null)
                                           .configure("constants", null)
                                           .configure("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
                                           .build();
    runner.init();
  }

  @Test(expected = StageException.class)
  public void testInitZeroLanePredicates() throws Exception {
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(null).addProcessor(new SelectorProcessor())
                                           .configure("lanePredicates", new HashMap())
                                           .configure("constants", null)
                                           .configure("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
                                           .build();
    runner.init();
  }

  @Test(expected = StageException.class)
  public void testInitLanePredicatesNotMatchingLanes() throws Exception {
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(null).addProcessor(new SelectorProcessor())
                                           .configure("lanePredicates", ImmutableMap.of("a", "true"))
                                           .configure("constants", null)
                                           .configure("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
                                           .build();
    runner.init();
  }

  @Test(expected = StageException.class)
  public void testInitLanePredicatesMoreOutputLanes() throws Exception {
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(null).addProcessor(new SelectorProcessor())
                                           .configure("lanePredicates", ImmutableMap.of("a", "true"))
                                           .configure("constants", null)
                                           .configure("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
                                           .outputLanes(ImmutableSet.of("a", "b"))
                                           .build();
    runner.init();
  }

  @Test(expected = StageException.class)
  public void testInitLanePredicatesInvalidPredicate() throws Exception {
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(null).addProcessor(new SelectorProcessor())
                                           .configure("lanePredicates", ImmutableMap.of("a", "x"))
                                           .configure("constants", null)
                                           .configure("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
                                           .outputLanes(ImmutableSet.of("a"))
                                           .build();
    runner.init();
  }

  @Test
  public void testInitLanePredicates() throws Exception {
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(null).addProcessor(new SelectorProcessor())
                                           .configure("lanePredicates", ImmutableMap.of("a", "true"))
                                           .configure("constants", ImmutableMap.of("x", "false"))
                                           .configure("constants", null)
                                           .configure("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
                                           .outputLanes(ImmutableSet.of("a"))
                                           .build();
    runner.init();
  }

  @Test
  public void testSelectWithDefault() throws Exception {
    RecordCreator records = new RecordCreator() {
      private int counter;
      @Override
      public Record create() {
        Record record = super.create();
        record.set(Field.create(counter++));
        return record;
      }
    };
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(records).addProcessor(new SelectorProcessor())
                                           .configure("lanePredicates", ImmutableMap.of("a", "record:value('') == 1",
                                                                                        "b", "record:value('') == 2",
                                                                                        "c", "default"))
                                           .configure("constants", ImmutableMap.of("x", "false"))
                                           .configure("constants", null)
                                           .configure("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
                                           .outputLanes(ImmutableSet.of("a", "b", "c"))
                                           .maxBatchSize(4)
                                           .build();
    runner.init();
    runner.process();
    Map<String, List<Record>> output = runner.getOutput();
    Assert.assertEquals(ImmutableSet.of("a", "b", "c"), output.keySet());
    Assert.assertEquals(1, output.get("a").size());
    Assert.assertEquals(1, output.get("a").get(0).get().getValueAsInteger());
    Assert.assertEquals(1, output.get("b").size());
    Assert.assertEquals(2, output.get("b").get(0).get().getValueAsInteger());
    Assert.assertEquals(2, output.get("c").size());
    Assert.assertEquals(0, output.get("c").get(0).get().getValueAsInteger());
    Assert.assertEquals(3, output.get("c").get(1).get().getValueAsInteger());
  }

  @Test
  public void testSelectWithoutDefaultDropping() throws Exception {
    RecordCreator records = new RecordCreator() {
      private int counter;
      @Override
      public Record create() {
        Record record = super.create();
        record.set(Field.create(counter++));
        return record;
      }
    };
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(records).addProcessor(new SelectorProcessor())
                                              .configure("lanePredicates", ImmutableMap.of("a", "record:value('') == 1",
                                                                                           "b", "record:value('') == 2"))
                                              .configure("constants", ImmutableMap.of("x", "false"))
                                              .configure("constants", null)
                                              .configure("onNoPredicateMatch", OnNoPredicateMatch.DROP_RECORD)
                                              .outputLanes(ImmutableSet.of("a", "b"))
                                              .maxBatchSize(4)
                                              .build();
    runner.init();
    runner.process();
    Map<String, List<Record>> output = runner.getOutput();
    Assert.assertEquals(ImmutableSet.of("a", "b"), output.keySet());
    Assert.assertEquals(1, output.get("a").size());
    Assert.assertEquals(1, output.get("a").get(0).get().getValueAsInteger());
    Assert.assertEquals(1, output.get("b").size());
    Assert.assertEquals(2, output.get("b").get(0).get().getValueAsInteger());
    Assert.assertTrue(runner.getErrors().isEmpty());
    Assert.assertTrue(runner.getErrorRecords().isEmpty());
  }

  @Test
  public void testSelectWithoutDefaultToError() throws Exception {
    RecordCreator records = new RecordCreator() {
      private int counter;
      @Override
      public Record create() {
        Record record = super.create();
        record.set(Field.create(counter++));
        return record;
      }
    };
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(records).addProcessor(new SelectorProcessor())
                                              .configure("lanePredicates", ImmutableMap.of("a", "record:value('') == 1",
                                                                                           "b", "record:value('') == 2"))
                                              .configure("constants", ImmutableMap.of("x", "false"))
                                              .configure("constants", null)
                                              .configure("onNoPredicateMatch", OnNoPredicateMatch.RECORD_TO_ERROR)
                                              .outputLanes(ImmutableSet.of("a", "b"))
                                              .maxBatchSize(4)
                                              .build();
    runner.init();
    runner.process();
    Map<String, List<Record>> output = runner.getOutput();
    Assert.assertEquals(ImmutableSet.of("a", "b"), output.keySet());
    Assert.assertEquals(1, output.get("a").size());
    Assert.assertEquals(1, output.get("a").get(0).get().getValueAsInteger());
    Assert.assertEquals(1, output.get("b").size());
    Assert.assertEquals(2, output.get("b").get(0).get().getValueAsInteger());
    Assert.assertTrue(runner.getErrors().isEmpty());
    Assert.assertEquals(2, runner.getErrorRecords().size());
    Assert.assertEquals(0, runner.getErrorRecords().get(0).get().getValueAsInteger());
    Assert.assertEquals(3, runner.getErrorRecords().get(1).get().getValueAsInteger());
  }

  @Test(expected = StageException.class)
  public void testSelectWithoutDefaultFailPipeline() throws Exception {
    RecordCreator records = new RecordCreator() {
      private int counter = 1;
      @Override
      public Record create() {
        Record record = super.create();
        record.set(Field.create(counter++));
        return record;
      }
    };
    ProcessorRunner<SelectorProcessor> runner =
        new ProcessorRunner.Builder<>(records).addProcessor(new SelectorProcessor())
                                              .configure("lanePredicates", ImmutableMap.of("a", "record:value('') == 1",
                                                                                           "b", "record:value('') == 2"))
                                              .configure("constants", ImmutableMap.of("x", "false"))
                                              .configure("constants", null)
                                              .configure("onNoPredicateMatch", OnNoPredicateMatch.FAIL_PIPELINE)
                                              .outputLanes(ImmutableSet.of("a", "b"))
                                              .maxBatchSize(4)
                                              .build();
    runner.init();
    try {
      runner.process();
    } finally {
      Map<String, List<Record>> output = runner.getOutput();
      Assert.assertEquals(ImmutableSet.of("a", "b"), output.keySet());
      Assert.assertEquals(1, output.get("a").size());
      Assert.assertEquals(1, output.get("a").get(0).get().getValueAsInteger());
      Assert.assertEquals(1, output.get("b").size());
      Assert.assertEquals(2, output.get("b").get(0).get().getValueAsInteger());
      Assert.assertTrue(runner.getErrors().isEmpty());
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
    }
  }

}
