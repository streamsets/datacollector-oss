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
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.InterceptorBean;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Record;

import com.streamsets.pipeline.api.interceptor.BaseInterceptor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPipeBatch {

  private PipelineBean getPipelineBean() {
    List<Issue> errors = new ArrayList<>();
    StageLibraryTask library = MockStages.createStageLibrary();
    PipelineConfiguration pipelineConf = MockStages.createPipelineConfigurationSourceTarget();
    PipelineBean pipelineBean = PipelineBeanCreator.get().create(false, library, pipelineConf, null, null, null, errors);
    if (pipelineBean == null) {
      Assert.fail(errors.toString());
    }
    return pipelineBean;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStageMethodsNoSnapshot() throws Exception {
    PipeBatch pipeBatch = new FullPipeBatch(null,null, -1, false);

    PipelineBean pipelineBean = getPipelineBean();
    StageRuntime[] stages = {
      new StageRuntime(pipelineBean, pipelineBean.getOrigin(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null),
      new StageRuntime(pipelineBean, pipelineBean.getPipelineStageBeans().getStages().get(0), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null,null)
    };

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.isPreview()).thenReturn(false);
    stages[0].setContext(context);
    stages[1].setContext(context);

    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe pipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
      LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT), Collections.EMPTY_LIST);

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(pipe);
    assertEquals(new ArrayList<String>(stageOutputLanes), batchMaker.getLanes());

    Record origRecord = new RecordImpl("i", "source", null, null);
    origRecord.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(origRecord, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    pipe = new StagePipe(stages[1], LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT),
      Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    // starting target
    batchMaker = pipeBatch.startStage(pipe);

    BatchImpl batch = pipeBatch.getBatch(pipe);

    // completing target
    pipeBatch.completeStage(batchMaker);

    Iterator<Record> records = batch.getRecords();
    Record recordFromBatch = records.next();

    Assert.assertNotSame(origRecord, recordFromBatch);
    assertEquals(origRecord.getHeader().getAttributeNames(), recordFromBatch.getHeader().getAttributeNames());
    assertEquals(origRecord.getHeader().getStageCreator(), recordFromBatch.getHeader().getStageCreator());
    assertEquals(origRecord.getHeader().getSourceId(), recordFromBatch.getHeader().getSourceId());
    assertEquals("s", recordFromBatch.getHeader().getStagesPath());
    Assert.assertNotEquals(origRecord.getHeader().getStagesPath(), recordFromBatch.getHeader().getStagesPath());
    Assert.assertNotEquals(origRecord.getHeader().getTrackingId(), recordFromBatch.getHeader().getTrackingId());

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
    PipeBatch pipeBatch = new FullPipeBatch(null,null, -1, true);

    PipelineBean pipelineBean = getPipelineBean();
    StageRuntime[] stages = {
      new StageRuntime(pipelineBean, pipelineBean.getOrigin(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null),
      new StageRuntime(pipelineBean, pipelineBean.getPipelineStageBeans().getStages().get(0), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null)
    };

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.isPreview()).thenReturn(false);
    stages[0].setContext(context);
    stages[1].setContext(context);

    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe sourcePipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
      LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT), Collections.EMPTY_LIST);

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(sourcePipe);
    assertEquals(new ArrayList<String>(stageOutputLanes), batchMaker.getLanes());

    Record origRecord = new RecordImpl("i", "source", null, null);
    origRecord.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(origRecord, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    StagePipe targetPipe = new StagePipe(stages[1], LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT),
      Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    // starting target
    batchMaker = pipeBatch.startStage(targetPipe);

    BatchImpl batch = pipeBatch.getBatch(targetPipe);

    // completing target
    pipeBatch.completeStage(batchMaker);

    Iterator<Record> records = batch.getRecords();
    Record recordFromBatch = records.next();

    Assert.assertNotSame(origRecord, recordFromBatch);
    assertEquals(origRecord.getHeader().getAttributeNames(), recordFromBatch.getHeader().getAttributeNames());
    assertEquals(origRecord.getHeader().getStageCreator(), recordFromBatch.getHeader().getStageCreator());
    assertEquals(origRecord.getHeader().getSourceId(), recordFromBatch.getHeader().getSourceId());
    assertEquals("s", recordFromBatch.getHeader().getStagesPath());
    Assert.assertNotEquals(origRecord.getHeader().getStagesPath(), recordFromBatch.getHeader().getStagesPath());
    Assert.assertNotEquals(origRecord.getHeader().getTrackingId(), recordFromBatch.getHeader().getTrackingId());

    assertEquals(origRecord.get(), recordFromBatch.get());
    assertEquals(origRecord.get(), recordFromBatch.get());
    assertEquals(origRecord.get(), recordFromBatch.get());
    assertEquals(origRecord.get(), recordFromBatch.get());

    Assert.assertTrue(recordFromBatch.getHeader().getStagesPath().
        endsWith(sourcePipe.getStage().getInfo().getInstanceName()));

    Assert.assertFalse(records.hasNext());

    List<StageOutput> stageOutputs = pipeBatch.getSnapshotsOfAllStagesOutput();
    assertNotNull(stageOutputs);
    assertEquals(2, stageOutputs.size());
    assertEquals("s", stageOutputs.get(0).getInstanceName());
    assertEquals(1, stageOutputs.get(0).getOutput().size());
    Record recordFromSnapshot = stageOutputs.get(0).getOutput().get(stageOutputLanes.get(0)).get(0);

    Assert.assertNotSame(origRecord, recordFromBatch);
    Assert.assertNotSame(origRecord, recordFromBatch);
    assertEquals(origRecord.getHeader().getAttributeNames(), recordFromBatch.getHeader().getAttributeNames());
    assertEquals(origRecord.getHeader().getStageCreator(), recordFromBatch.getHeader().getStageCreator());
    assertEquals(origRecord.getHeader().getSourceId(), recordFromBatch.getHeader().getSourceId());
    assertEquals("s", recordFromBatch.getHeader().getStagesPath());
    Assert.assertNotEquals(origRecord.getHeader().getStagesPath(), recordFromBatch.getHeader().getStagesPath());
    Assert.assertNotEquals(origRecord.getHeader().getTrackingId(), recordFromBatch.getHeader().getTrackingId());

    Assert.assertNotSame(origRecord, recordFromSnapshot);
    assertEquals(origRecord.getHeader().getAttributeNames(), recordFromSnapshot.getHeader().getAttributeNames());
    assertEquals(origRecord.getHeader().getStageCreator(), recordFromSnapshot.getHeader().getStageCreator());
    assertEquals(origRecord.getHeader().getSourceId(), recordFromSnapshot.getHeader().getSourceId());
    assertEquals("s", recordFromSnapshot.getHeader().getStagesPath());
    Assert.assertNotEquals(origRecord.getHeader().getStagesPath(), recordFromSnapshot.getHeader().getStagesPath());
    Assert.assertNotEquals(origRecord.getHeader().getTrackingId(), recordFromSnapshot.getHeader().getTrackingId());

    assertEquals(recordFromBatch, recordFromSnapshot);
    Assert.assertNotSame(recordFromBatch, recordFromSnapshot);

    assertEquals("t", stageOutputs.get(1).getInstanceName());
    assertEquals(0, stageOutputs.get(1).getOutput().size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMoveLane() throws Exception {
    FullPipeBatch pipeBatch = new FullPipeBatch(null, null, -1, true);

    PipelineBean pipelineBean = getPipelineBean();
    StageRuntime[] stages = {
      new StageRuntime(pipelineBean, pipelineBean.getOrigin(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null),
      new StageRuntime(pipelineBean, pipelineBean.getPipelineStageBeans().getStages().get(0), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null)
    };

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.isPreview()).thenReturn(false);
    stages[0].setContext(context);

    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe pipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
      LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT), Collections.EMPTY_LIST);

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(pipe);
    assertEquals(new ArrayList<String>(stageOutputLanes), batchMaker.getLanes());

    Record record = new RecordImpl("i", "source", null, null);
    record.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(record, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    Record origRecord = pipeBatch.getFullPayload().get(pipe.getOutputLanes().get(0)).get(0);
    pipeBatch.moveLane(pipe.getOutputLanes().get(0), "x");
    Record movedRecord = pipeBatch.getFullPayload().get("x").get(0);
    Assert.assertSame(origRecord, movedRecord);

    Map<String, List<Record>> snapshot = pipeBatch.getLaneOutputRecords(ImmutableList.of("x"));
    assertEquals(1, snapshot.size());
    assertEquals(1, snapshot.get("x").size());
    assertEquals("A", snapshot.get("x").get(0).getHeader().getAttribute("a"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMoveLaneCopying() throws Exception {
    FullPipeBatch pipeBatch = new FullPipeBatch(null, null, -1, true);

    PipelineBean pipelineBean = getPipelineBean();
    StageRuntime[] stages = {
      new StageRuntime(pipelineBean, pipelineBean.getOrigin(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null),
      new StageRuntime(pipelineBean, pipelineBean.getPipelineStageBeans().getStages().get(0), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null)
    };

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.isPreview()).thenReturn(false);
    stages[0].setContext(context);

    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe pipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
      LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT), Collections.EMPTY_LIST);

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(pipe);
    assertEquals(new ArrayList<String>(stageOutputLanes), batchMaker.getLanes());

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

    assertEquals(origRecord, copiedRecordX);
    Assert.assertSame(origRecord, copiedRecordX);

    assertEquals(origRecord, copiedRecordY);
    Assert.assertNotSame(origRecord, copiedRecordY);


    Map<String, List<Record>> snapshot = pipeBatch.getLaneOutputRecords(list);
    assertEquals(2, snapshot.size());
    assertEquals(1, snapshot.get("x").size());
    assertEquals(1, snapshot.get("y").size());
    assertEquals("A", snapshot.get("x").get(0).getHeader().getAttribute("a"));
    assertEquals("A", snapshot.get("y").get(0).getHeader().getAttribute("a"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOverride() throws Exception {
    PipeBatch pipeBatch = new FullPipeBatch(null, null, -1, true);

    PipelineBean pipelineBean = getPipelineBean();
    StageRuntime[] stages = {
      new StageRuntime(pipelineBean, pipelineBean.getOrigin(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null),
      new StageRuntime(pipelineBean, pipelineBean.getPipelineStageBeans().getStages().get(0), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null,null)
    };

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.isPreview()).thenReturn(false);
    stages[0].setContext(context);
    stages[1].setContext(context);

    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe sourcePipe = new StagePipe(stages[0], Collections.EMPTY_LIST,
      LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT), Collections.EMPTY_LIST);

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(sourcePipe);
    assertEquals(new ArrayList<String>(stageOutputLanes), batchMaker.getLanes());

    Record origRecord = new RecordImpl("i", "source", null, null);
    origRecord.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(origRecord, stageOutputLanes.get(0));

    // completing source
    pipeBatch.completeStage(batchMaker);

    StagePipe targetPipe = new StagePipe(stages[1], LaneResolver.getPostFixed(stageOutputLanes,
      LaneResolver.STAGE_OUT), Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    // starting target
    batchMaker = pipeBatch.startStage(targetPipe);

    BatchImpl batch = pipeBatch.getBatch(targetPipe);

    // completing target
    pipeBatch.completeStage(batchMaker);

    // getting stages ouptut
    List<StageOutput> stageOutputs = pipeBatch.getSnapshotsOfAllStagesOutput();

    StageOutput sourceOutput = stageOutputs.get(0);
    assertEquals("s", sourceOutput.getInstanceName());

    Record modRecord = new RecordImpl("i", "source", null, null);
    modRecord.getHeader().setAttribute("a", "B");
    //modifying the source output
    sourceOutput.getOutput().get(stages[0].getConfiguration().getOutputLanes().get(0)).set(0, modRecord);

    //starting a new pipe batch
    pipeBatch = new FullPipeBatch(null, null, -1, true);

    //instead running source, we inject its previous-modified output, it implicitly starts the pipe
    pipeBatch.overrideStageOutput(sourcePipe, sourceOutput);

    // starting target
    pipeBatch.startStage(targetPipe);
    batch = pipeBatch.getBatch(targetPipe);
    Iterator<Record> it = batch.getRecords();
    Record tRecord = it.next();
    //check that we get the injected record.
    assertEquals("B", tRecord.getHeader().getAttribute("a"));
    Assert.assertFalse(it.hasNext());

    // completing target
    pipeBatch.completeStage(batchMaker);
  }

  private static class DummyInterceptor extends InterceptorRuntime {
    private final String mark;

    public DummyInterceptor(String mark)  {
      super(null, null);
      this.mark = mark;
    }

    @Override
    public List<Record> intercept(List<Record> records) {
      for(Record record: records) {
        record.getHeader().setAttribute(mark, "passed");
      }

      return records;
    }
  }

  @Test
  public void testInterceptors() throws Exception {
    PipeBatch pipeBatch = new FullPipeBatch(null,null, -1, true);

    PipelineBean pipelineBean = getPipelineBean();
    StageRuntime[] stages = {
      new StageRuntime(pipelineBean, pipelineBean.getOrigin(), Collections.emptyList(), Collections.emptyList(), Collections.singletonList(new DummyInterceptor("source")), null, null),
      new StageRuntime(pipelineBean, pipelineBean.getPipelineStageBeans().getStages().get(0), Collections.emptyList(), Collections.singletonList(new DummyInterceptor("target")), Collections.emptyList(), null, null)
    };

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.isPreview()).thenReturn(false);
    stages[0].setContext(context);
    stages[1].setContext(context);

    List<String> stageOutputLanes = stages[0].getConfiguration().getOutputLanes();
    StagePipe sourcePipe = new StagePipe(
      stages[0],
      Collections.emptyList(),
      LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT),
      Collections.emptyList()
    );

    // starting source
    BatchMakerImpl batchMaker = pipeBatch.startStage(sourcePipe);
    assertEquals(new ArrayList<>(stageOutputLanes), batchMaker.getLanes());

    Record origRecord = new RecordImpl("i", "source", null, null);
    origRecord.getHeader().setAttribute("a", "A");
    batchMaker.addRecord(origRecord, stageOutputLanes.get(0));

    // completing source
//    pipeBatch.completeStage(batchMaker,
    pipeBatch.completeStage(batchMaker);

    StagePipe targetPipe = new StagePipe(
      stages[1],
      LaneResolver.getPostFixed(stageOutputLanes, LaneResolver.STAGE_OUT),
      Collections.emptyList(),
      Collections.emptyList()
    );

    // starting target
    batchMaker = pipeBatch.startStage(targetPipe);

//    BatchImpl batch = pipeBatch.getBatch(targetPipe, Collections.singletonList(new DummyInterceptor("target")));
    BatchImpl batch = pipeBatch.getBatch(targetPipe);

    Record record = batch.getRecords().next();
    assertNotNull(record);
    assertEquals("passed", record.getHeader().getAttribute("source"));
    assertEquals("passed", record.getHeader().getAttribute("target"));

    // completing target
    pipeBatch.completeStage(batchMaker);
  }
}
