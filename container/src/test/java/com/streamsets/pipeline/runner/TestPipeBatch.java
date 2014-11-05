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

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.record.TestRecordImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestPipeBatch {

  @Test
  @SuppressWarnings("unchecked")
  public void testSourceOffsetTracker() throws Exception {
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    Mockito.when(tracker.getOffset()).thenReturn("foo");
    PipeBatch pipeBatch = new PipeBatch(tracker, new MetricRegistry(), -1, false);
    Assert.assertEquals("foo", pipeBatch.getPreviousOffset());
    Mockito.verify(tracker, Mockito.times(1)).getOffset();
    Mockito.verifyNoMoreInteractions(tracker);
    pipeBatch.setNewOffset("bar");
    Mockito.verify(tracker, Mockito.times(1)).setOffset(Mockito.eq("bar"));
    Mockito.verifyNoMoreInteractions(tracker);

    pipeBatch.commitOffset();
    Mockito.verify(tracker, Mockito.times(1)).commitOffset();
    Mockito.verifyNoMoreInteractions(tracker);

    StageRuntime[] stages = new StageRuntime.Builder(MockStages.createStageLibrary(),
                                                    MockStages.createPipelineConfigurationSourceTarget()).build();

    StagePipe pipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
                                             LaneResolver.getPostFixed(stages[0].getConfiguration().getOutputLanes(),
                                                                       LaneResolver.STAGE_OUT));

    Batch batch = pipeBatch.getBatch(pipe);
    Assert.assertEquals("foo", batch.getSourceOffset());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStageMethodsNoSnapshot() throws Exception {
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipeBatch pipeBatch = new PipeBatch(tracker, new MetricRegistry(), -1, false);

    StageRuntime[] stages = new StageRuntime.Builder(MockStages.createStageLibrary(),
                                                     MockStages.createPipelineConfigurationSourceTarget()).build();
    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe pipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
                                   LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT));

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(pipe);
    Assert.assertEquals(new HashSet<String>(stageOutputLanes), batchMaker.getLanes());

    Record origRecord = new RecordImpl("i", "source", null, null);
    origRecord.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(origRecord, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    pipe = new StagePipe(stages[1], LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT),
                         Collections.EMPTY_LIST);

    // starting target
    batchMaker = pipeBatch.startStage(pipe);

    BatchImpl batch = pipeBatch.getBatch(pipe);

    // completing target
    pipeBatch.completeStage(batchMaker);

    Iterator<Record> records = batch.getRecords();
    Record recordFromBatch = records.next();

    TestRecordImpl.assertIsCopy(origRecord, recordFromBatch, false);

    Assert.assertFalse(records.hasNext());
    Assert.assertNull(pipeBatch.getSnapshotsOfAllStagesOutput());

    try {
      pipeBatch.startStage(pipe);
      Assert.fail();
    } catch (IllegalStateException ex) {
      //expected
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStageMethodsWithSnapshot() throws Exception {
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipeBatch pipeBatch = new PipeBatch(tracker, new MetricRegistry(), -1, true);

    StageRuntime[] stages = new StageRuntime.Builder(MockStages.createStageLibrary(),
                                                     MockStages.createPipelineConfigurationSourceTarget()).build();
    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe sourcePipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
                                   LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT));

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(sourcePipe);
    Assert.assertEquals(new HashSet<String>(stageOutputLanes), batchMaker.getLanes());

    Record origRecord = new RecordImpl("i", "source", null, null);
    origRecord.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(origRecord, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    StagePipe targetPipe = new StagePipe(stages[1], LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT),
                         Collections.EMPTY_LIST);

    // starting target
    batchMaker = pipeBatch.startStage(targetPipe);

    BatchImpl batch = pipeBatch.getBatch(targetPipe);

    // completing target
    pipeBatch.completeStage(batchMaker);

    Iterator<Record> records = batch.getRecords();
    Record recordFromBatch = records.next();

    TestRecordImpl.assertIsCopy(origRecord, recordFromBatch, false);
    Assert.assertTrue(recordFromBatch.getHeader().getStagesPath().
        endsWith(sourcePipe.getStage().getInfo().getInstanceName()));

    Assert.assertFalse(records.hasNext());

    List<StageOutput> stageOutputs = pipeBatch.getSnapshotsOfAllStagesOutput();
    Assert.assertNotNull(stageOutputs);
    Assert.assertEquals(2, stageOutputs.size());
    Assert.assertEquals("s", stageOutputs.get(0).getInstanceName());
    Assert.assertEquals(1, stageOutputs.get(0).getOutput().size());
    Record recordFromSnapshot = stageOutputs.get(0).getOutput().get(stageOutputLanes.get(0)).get(0);

    TestRecordImpl.assertIsSnapshot(recordFromBatch, recordFromSnapshot);
    TestRecordImpl.assertIsCopy(origRecord, recordFromSnapshot, false);

    Assert.assertEquals("t", stageOutputs.get(1).getInstanceName());
    Assert.assertEquals(0, stageOutputs.get(1).getOutput().size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMoveLane() throws Exception {
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipeBatch pipeBatch = new PipeBatch(tracker, new MetricRegistry(), -1, true);

    StageRuntime[] stages = new StageRuntime.Builder(MockStages.createStageLibrary(),
                                                     MockStages.createPipelineConfigurationSourceTarget()).build();
    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe pipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
                                   LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT));

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(pipe);
    Assert.assertEquals(new HashSet<String>(stageOutputLanes), batchMaker.getLanes());

    Record record = new RecordImpl("i", "source", null, null);
    record.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(record, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    Record origRecord = pipeBatch.getFullPayload().get(pipe.getOutputLanes().get(0)).get(0);
    pipeBatch.moveLane(pipe.getOutputLanes().get(0), "x");
    Record movedRecord = pipeBatch.getFullPayload().get("x").get(0);
    Assert.assertSame(origRecord, movedRecord);

    Map<String, List<Record>> snapshot = pipeBatch.getPipeLanesSnapshot(ImmutableList.of("x"));
    Assert.assertEquals(1, snapshot.size());
    Assert.assertEquals(1, snapshot.get("x").size());
    Assert.assertEquals("A", snapshot.get("x").get(0).getHeader().getAttribute("a"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMoveLaneCopying() throws Exception {
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipeBatch pipeBatch = new PipeBatch(tracker, new MetricRegistry(), -1, true);

    StageRuntime[] stages = new StageRuntime.Builder(MockStages.createStageLibrary(),
                                                     MockStages.createPipelineConfigurationSourceTarget()).build();
    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe pipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
                                   LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT));

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(pipe);
    Assert.assertEquals(new HashSet<String>(stageOutputLanes), batchMaker.getLanes());

    Record record = new RecordImpl("i", "source", null, null);
    record.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(record, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    List<String> list = ImmutableList.of("x", "y");


    Record origRecord = pipeBatch.getFullPayload().get(pipe.getOutputLanes().get(0)).get(0);
    pipeBatch.moveLaneCopying(pipe.getOutputLanes().get(0), list);
    Record copiedRecordX = pipeBatch.getFullPayload().get("x").get(0);
    Record copiedRecordY = pipeBatch.getFullPayload().get("y").get(0);
    TestRecordImpl.assertIsCopy(origRecord, copiedRecordX, true);
    TestRecordImpl.assertIsCopy(origRecord, copiedRecordY, true);


    Map<String, List<Record>> snapshot = pipeBatch.getPipeLanesSnapshot(list);
    Assert.assertEquals(2, snapshot.size());
    Assert.assertEquals(1, snapshot.get("x").size());
    Assert.assertEquals(1, snapshot.get("y").size());
    Assert.assertEquals("A", snapshot.get("x").get(0).getHeader().getAttribute("a"));
    Assert.assertEquals("A", snapshot.get("y").get(0).getHeader().getAttribute("a"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCombineLanes() throws Exception {
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipeBatch pipeBatch = new PipeBatch(tracker, new MetricRegistry(), -1, true);

    StageRuntime[] stages = new StageRuntime.Builder(MockStages.createStageLibrary(),
                                                     MockStages.createPipelineConfigurationSourceTarget()).build();
    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe pipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
                                   LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT));

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(pipe);
    Assert.assertEquals(new HashSet<String>(stageOutputLanes), batchMaker.getLanes());

    Record record = new RecordImpl("i", "source", null, null);
    record.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(record, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    List<String> list = ImmutableList.of("x", "y");
    pipeBatch.moveLaneCopying(pipe.getOutputLanes().get(0), list);

    Record copiedRecordX = pipeBatch.getFullPayload().get("x").get(0);
    Record copiedRecordY = pipeBatch.getFullPayload().get("y").get(0);

    pipeBatch.combineLanes(list, "z");
    Map<String, List<Record>> snapshot = pipeBatch.getPipeLanesSnapshot(ImmutableList.of("z"));

    Assert.assertEquals(1, snapshot.size());
    Assert.assertEquals(2, snapshot.get("z").size());

    Assert.assertSame(copiedRecordX, pipeBatch.getFullPayload().get("z").get(0));
    Assert.assertSame(copiedRecordY, pipeBatch.getFullPayload().get("z").get(1));
  }


}
