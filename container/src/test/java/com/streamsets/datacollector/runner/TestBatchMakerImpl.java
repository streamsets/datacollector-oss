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
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;

import com.streamsets.pipeline.api.StageType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestBatchMakerImpl {

  @SuppressWarnings("unchecked")
  private StagePipe createStagePipe(List<String> outputs) {
    StageConfiguration stageConfiguration = new StageConfigurationBuilder("i",  "n")
      .withOutputLanes("i")
      .withOutputLanes(outputs)
      .build();
    StageDefinition stageDef = Mockito.mock(StageDefinition.class);
    Mockito.when(stageDef.getType()).thenReturn(StageType.SOURCE);
    StageRuntime stageRuntime = Mockito.mock(StageRuntime.class);
    Stage.Info stageInfo = Mockito.mock(Stage.Info.class);
    Mockito.when(stageInfo.getInstanceName()).thenReturn("i");
    Mockito.when(stageInfo.getName()).thenReturn("n");
    Mockito.when(stageInfo.getVersion()).thenReturn(1);
    Mockito.when(stageRuntime.getInfo()).thenReturn(stageInfo);
    Mockito.when(stageRuntime.getConfiguration()).thenReturn(stageConfiguration);
    Mockito.when(stageRuntime.getDefinition()).thenReturn(stageDef);
    List<String> pipeInput = LaneResolver.getPostFixed(ImmutableList.of("a"), LaneResolver.MULTIPLEXER_OUT);
    List<String> pipeOutputs = new ArrayList<String>();
    for (String output : outputs) {
      pipeOutputs.add(LaneResolver.createLane(output, "x", "x"));
    }
    List<String> pipeOutput = LaneResolver.getPostFixed(pipeOutputs, LaneResolver.STAGE_OUT);

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.isPreview()).thenReturn(false);
    Mockito.when(stageRuntime.getContext()).thenReturn(context);
    return new StagePipe(stageRuntime, pipeInput, pipeOutput, Collections.<String>emptyList());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchMakerSingleOutputNoSnapshot() {
    StagePipe pipe = createStagePipe(ImmutableList.of("o"));
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, false);
    Assert.assertEquals(pipe, batchMaker.getStagePipe());
    Assert.assertEquals(1, batchMaker.getStageOutput().size());
    Assert.assertEquals(ImmutableMap.of("o", Collections.EMPTY_LIST), batchMaker.getStageOutput());
    Assert.assertNull(batchMaker.getStageOutputSnapshot());
    Assert.assertEquals(ImmutableList.of("o"), batchMaker.getLanes());

    Record record1 = new RecordImpl("i", "source", null, null);
    record1.getHeader().setAttribute("r", "1");
    batchMaker.addRecord(record1);
    Record record2 = new RecordImpl("i", "source", null, null);
    record2.getHeader().setAttribute("r", "2");
    batchMaker.addRecord(record2, "o");
    Record record3 = new RecordImpl("i", "source", null, null);
    record3.getHeader().setAttribute("r", "3");
    try {
      batchMaker.addRecord(record3, "x");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    try {
      batchMaker.addRecord(record3, "o", "o");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    List<Record> records = batchMaker.getStageOutput().get("o");
    Assert.assertEquals(2, records.size());
    Assert.assertEquals("1", records.get(0).getHeader().getAttribute("r"));
    Assert.assertEquals("2", records.get(1).getHeader().getAttribute("r"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchMakerSingleOutputWithSnapshot() {
    StagePipe pipe = createStagePipe(ImmutableList.of("o"));
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, true);
    Assert.assertNotNull(batchMaker.getStageOutputSnapshot());

    Record record1 = new RecordImpl("i", "source", null, null);
    record1.getHeader().setAttribute("r", "1");
    batchMaker.addRecord(record1);
    Record record2 = new RecordImpl("i", "source", null, null);
    record2.getHeader().setAttribute("r", "2");
    batchMaker.addRecord(record2, "o");
    List<Record> records = batchMaker.getStageOutputSnapshot().get("o");
    Assert.assertEquals(2, records.size());
    Assert.assertEquals("1", records.get(0).getHeader().getAttribute("r"));
    Assert.assertEquals("2", records.get(1).getHeader().getAttribute("r"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchMakerMultipleOutputNoSnapshot() {
    StagePipe pipe = createStagePipe(ImmutableList.of("o1", "o2"));
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, false);
    Assert.assertEquals(pipe, batchMaker.getStagePipe());
    Assert.assertEquals(2, batchMaker.getStageOutput().size());
    Assert.assertEquals(ImmutableMap.of("o1", Collections.EMPTY_LIST, "o2", Collections.EMPTY_LIST),
                        batchMaker.getStageOutput());
    Assert.assertNull(batchMaker.getStageOutputSnapshot());
    Assert.assertEquals(ImmutableList.of("o1", "o2"), batchMaker.getLanes());

    Record record1 = new RecordImpl("i", "source", null, null);
    record1.getHeader().setAttribute("r", "1");
    batchMaker.addRecord(record1, "o1");
    Record record2 = new RecordImpl("i", "source", null, null);
    record2.getHeader().setAttribute("r", "2");
    batchMaker.addRecord(record2, "o2");
    Record record3 = new RecordImpl("i", "source", null, null);
    record3.getHeader().setAttribute("r", "3");
    try {
      batchMaker.addRecord(record3, "o1", "o1");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    try {
      batchMaker.addRecord(record3, "x");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    try {
      batchMaker.addRecord(record3);
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    batchMaker.addRecord(record3, "o1", "o2");
    List<Record> records = batchMaker.getStageOutput().get("o1");
    Assert.assertEquals(2, records.size());
    Assert.assertEquals("1", records.get(0).getHeader().getAttribute("r"));
    Assert.assertEquals("3", records.get(1).getHeader().getAttribute("r"));
    records = batchMaker.getStageOutput().get("o2");
    Assert.assertEquals(2, records.size());
    Assert.assertEquals("2", records.get(0).getHeader().getAttribute("r"));
    Assert.assertEquals("3", records.get(1).getHeader().getAttribute("r"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchMakerMultipleOutputWithSnapshot() {
    StagePipe pipe = createStagePipe(ImmutableList.of("o1", "o2"));
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, true);
    Assert.assertNotNull(batchMaker.getStageOutputSnapshot());

    Record record1 = new RecordImpl("i", "source", null, null);
    record1.getHeader().setAttribute("r", "1");
    batchMaker.addRecord(record1, "o1");
    Record record2 = new RecordImpl("i", "source", null, null);
    record2.getHeader().setAttribute("r", "2");
    batchMaker.addRecord(record2, "o2");
    List<Record> records = batchMaker.getStageOutputSnapshot().get("o1");
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("1", records.get(0).getHeader().getAttribute("r"));
    records = batchMaker.getStageOutputSnapshot().get("o2");
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("2", records.get(0).getHeader().getAttribute("r"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMaxRecordsWithInLimit() {
    StagePipe pipe = createStagePipe(ImmutableList.of("o"));
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, false, 1);
    Record record = new RecordImpl("i", "source", null, null);
    batchMaker.addRecord(record);
  }

  @Test()
  @SuppressWarnings("unchecked")
  public void testMaxRecordsBeyondLimit() {
    StagePipe pipe = createStagePipe(ImmutableList.of("o"));
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, false, 1);
    Record record = new RecordImpl("i", "source", null, null);
    batchMaker.addRecord(record);
    batchMaker.addRecord(record);
    //This is changed to log warning and not throw IllegalArgumentException.
    //Some sources could translate one event to multiple records and my go over the batch size.
  }

  @Test
  public void testRecordByRef() {
    StageConfiguration stageConfiguration = new StageConfigurationBuilder("i","n")
      .withOutputLanes("o")
      .build();
    StageDefinition stageDef = Mockito.mock(StageDefinition.class);
    Mockito.when(stageDef.getType()).thenReturn(StageType.SOURCE);
    Source.Context context = Mockito.mock(Source.Context.class);

    Mockito.when(stageDef.getType()).thenReturn(StageType.SOURCE);

    Stage.Info stageInfo = Mockito.mock(Stage.Info.class);
    Mockito.when(stageInfo.getInstanceName()).thenReturn("i");

    StageRuntime stageRuntime = Mockito.mock(StageRuntime.class);
    Mockito.when(stageRuntime.getInfo()).thenReturn(stageInfo);
    Mockito.when(stageRuntime.getConfiguration()).thenReturn(stageConfiguration);
    Mockito.when(stageRuntime.getDefinition()).thenReturn(stageDef);
    Mockito.when(stageRuntime.getContext()).thenReturn(context);

    StagePipe pipe = new StagePipe(stageRuntime, Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList());

    Record record = new RecordImpl("s", "s", null, null);

    // preview by value
    Mockito.when(context.isPreview()).thenReturn(true);
    Mockito.when(stageDef.getRecordsByRef()).thenReturn(false);
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, false);
    Assert.assertFalse(batchMaker.isRecordByRef());
    Assert.assertNotSame(record, batchMaker.getRecordForBatchMaker(record));

    // preview by ref
    Mockito.when(context.isPreview()).thenReturn(true);
    Mockito.when(stageDef.getRecordsByRef()).thenReturn(true);
    batchMaker = new BatchMakerImpl(pipe, false);
    Assert.assertFalse(batchMaker.isRecordByRef());
    Assert.assertNotSame(record, batchMaker.getRecordForBatchMaker(record));

    // prod by value
    Mockito.when(stageDef.getRecordsByRef()).thenReturn(false);
    Mockito.when(context.isPreview()).thenReturn(false);
    batchMaker = new BatchMakerImpl(pipe, false);
    Assert.assertFalse(batchMaker.isRecordByRef());
    Assert.assertNotSame(record, batchMaker.getRecordForBatchMaker(record));

    // prod by ref
    Mockito.when(stageDef.getRecordsByRef()).thenReturn(true);
    Mockito.when(context.isPreview()).thenReturn(false);
    batchMaker = new BatchMakerImpl(pipe, false);
    Assert.assertTrue(batchMaker.isRecordByRef());
    Assert.assertSame(record, batchMaker.getRecordForBatchMaker(record));

  }

}
