/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.util.ContainerError;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;


public class TestPreviewRun {

  @Before
  public void setUp() {
    MockStages.resetStageCaptures();
  }

  @Test
  public void testPreviewRun() throws Exception {
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.set(Field.create(1));
        batchMaker.addRecord(record);
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.set(Field.create(2));
        batchMaker.addRecord(record);
      }
    });
    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
      }
    });
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipelineRunner runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), "name",
                                             MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);
    pipeline.init();
    pipeline.run();
    pipeline.destroy();
    List<StageOutput> output = runner.getBatchesOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).get().getValue());
  }

  @Test
  public void testPreviewPipelineBuilder() throws Exception {
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.set(Field.create(1));
        batchMaker.addRecord(record);
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.set(Field.create(2));
        batchMaker.addRecord(record);
      }
    });
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    pipelineConfiguration.getStages().remove(2);

    PreviewPipeline pipeline = new PreviewPipelineBuilder(MockStages.createStageLibrary(), "name",
      pipelineConfiguration, null).build(runner);
    PreviewPipelineOutput previewOutput = pipeline.run();
    List<StageOutput> output = previewOutput.getBatchesOutput().get(0);
    Assert.assertEquals(2, output.size());
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).get().getValue());
  }


  @Test
  public void testPreviewPipelineBuilderWithLastStage() throws Exception {
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.set(Field.create(1));
        batchMaker.addRecord(record);
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.set(Field.create(2));
        batchMaker.addRecord(record);
      }
    });

    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();

    PreviewPipeline pipeline = new PreviewPipelineBuilder(MockStages.createStageLibrary(), "name",
      pipelineConfiguration, "p").build(runner);

    PreviewPipelineOutput previewOutput = pipeline.run();
    List<StageOutput> output = previewOutput.getBatchesOutput().get(0);

    Assert.assertEquals(1, output.size());
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());
  }


  @Test
  public void testIsPreview() throws Exception {
    MockStages.setSourceCapture(new BaseSource() {

      @Override
      protected void init() throws StageException {
        super.init();
        Assert.assertTrue(getContext().isPreview());
      }

      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        return "X";
      }
    });

    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    pipelineConfiguration.getStages().remove(2);

    PreviewPipeline pipeline = new PreviewPipelineBuilder(MockStages.createStageLibrary(), "name",
                                                          pipelineConfiguration, null).build(runner);
    PreviewPipelineOutput previewOutput = pipeline.run();
    Mockito.verify(tracker).setOffset(Mockito.eq("X"));
  }

  @Test(expected = PipelineRuntimeException.class)
  public void testPreviewRunFailValidationConfigs() throws Exception {

    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> validateConfigs(Info info, Source.Context context) {
        return Arrays.asList(context.createConfigIssue(null, null, ContainerError.CONTAINER_0000));
      }

      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.set(Field.create(2));
        batchMaker.addRecord(record);
      }
    });
    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
      }
    });
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipelineRunner runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    new PreviewPipelineBuilder(MockStages.createStageLibrary(), "name",
                         MockStages.createPipelineConfigurationSourceProcessorTarget(), null).build(runner);
  }


  @Test
  public void testPreviewRunOverride() throws Exception {
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.set(Field.create(1));
        batchMaker.addRecord(record);
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        int currentValue = record.get().getValueAsInteger();
        record.set(Field.create(currentValue * 2));
        batchMaker.addRecord(record);
      }
    });
    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
      }
    });
    PipelineConfiguration pipelineConf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PipelineRunner runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), "name", pipelineConf).build(runner);
    pipeline.init();
    pipeline.run();
    pipeline.destroy();
    List<StageOutput> output = runner.getBatchesOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).get().getValue());

    StageOutput sourceOutput = output.get(0);
    Assert.assertEquals("s", sourceOutput.getInstanceName());

    Record modRecord = new RecordImpl("i", "source", null, null);
    modRecord.set(Field.create(10));
    //modifying the source output
    sourceOutput.getOutput().get(pipelineConf.getStages().get(0).getOutputLanes().get(0)).set(0, modRecord);

    runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), "name",
                                    MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    pipeline.init();
    pipeline.run(Arrays.asList(sourceOutput));
    pipeline.destroy();
    output = runner.getBatchesOutput().get(0);
    Assert.assertEquals(10, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(20, output.get(1).getOutput().get("p").get(0).get().getValue());
  }

}
