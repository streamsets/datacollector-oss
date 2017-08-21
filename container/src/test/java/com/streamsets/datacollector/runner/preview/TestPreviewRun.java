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
package com.streamsets.datacollector.runner.preview;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.runner.common.PipelineStopReason;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.StageOutputJson;
import com.streamsets.datacollector.runner.MockPipelineBuilder;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class TestPreviewRun {
  private Configuration configuration;
  private RuntimeInfo runtimeInfo;

  @Before
  public void setUp() {
    MockStages.resetStageCaptures();
    configuration = new Configuration();
    runtimeInfo = Mockito.mock(RuntimeInfo.class);
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
    PipelineRunner runner = new PreviewPipelineRunner( "name", "0", runtimeInfo, tracker, -1, 1, true, true);
    Pipeline pipeline = new MockPipelineBuilder()
      .withPipelineConf(MockStages.createPipelineConfigurationSourceProcessorTarget())
      .build(runner);
    pipeline.init(false);
    pipeline.run();
    pipeline.destroy(false, PipelineStopReason.UNUSED);
    List<StageOutput> output = runner.getBatchesOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).get().getValue());

    // Test serializing to json and json to bean
    List<StageOutputJson> stageOutputJsonList = BeanHelper.wrapStageOutput(output);
    String outputStr = ObjectMapperFactory.get().writeValueAsString(stageOutputJsonList.get(0));
    StageOutputJson stageOutputJson = ObjectMapperFactory.get().readValue(outputStr, StageOutputJson.class);
    Assert.assertNotNull(stageOutputJson);
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
    PreviewPipelineRunner runner = new PreviewPipelineRunner( "name", "0", runtimeInfo, tracker, -1, 1, true, true);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    pipelineConfiguration.getStages().remove(2);

    PreviewPipeline pipeline = new PreviewPipelineBuilder(
      MockStages.createStageLibrary(),
      configuration,
      "name",
      "0",
      pipelineConfiguration,
      null,
      Mockito.mock(LineagePublisherTask.class)
    ).build(MockStages.userContext(), runner);
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
        //batchMaker.addRecord(record);
      }
    });

    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PreviewPipelineRunner runner = new PreviewPipelineRunner( "name", "0",runtimeInfo, tracker, -1, 1, true, true);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();

    PreviewPipeline pipeline = new PreviewPipelineBuilder(
      MockStages.createStageLibrary(),
      configuration,
      "name",
      "0",
      pipelineConfiguration,
      "p",
      Mockito.mock(LineagePublisherTask.class)
    ).build(MockStages.userContext(), runner);

    PreviewPipelineOutput previewOutput = pipeline.run();
    List<StageOutput> output = previewOutput.getBatchesOutput().get(0);

    Assert.assertEquals(1, output.size());


    //Complex graph
    runner = new PreviewPipelineRunner("name", "0", runtimeInfo, tracker, -1, 1, true, true);
    pipelineConfiguration = MockStages.createPipelineConfigurationComplexSourceProcessorTarget();
    pipeline = new PreviewPipelineBuilder(
      MockStages.createStageLibrary(),
      configuration,
      "name",
      "0",
      pipelineConfiguration,
      "p1",
      Mockito.mock(LineagePublisherTask.class)
    ).build(MockStages.userContext(), runner);
    previewOutput = pipeline.run();
    output = previewOutput.getBatchesOutput().get(0);
    Assert.assertEquals(1, output.size());
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());


    runner = new PreviewPipelineRunner("name", "0", runtimeInfo, tracker, -1, 1, true, true);
    pipelineConfiguration = MockStages.createPipelineConfigurationComplexSourceProcessorTarget();
    pipeline = new PreviewPipelineBuilder(
      MockStages.createStageLibrary(),
      configuration,
      "name",
      "0",
      pipelineConfiguration,
      "p5",
      Mockito.mock(LineagePublisherTask.class)
    ).build(MockStages.userContext(), runner);
    previewOutput = pipeline.run();
    output = previewOutput.getBatchesOutput().get(0);
    Assert.assertEquals(2, output.size());
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());


    runner = new PreviewPipelineRunner("name", "0", runtimeInfo, tracker, -1, 1, true, true);
    pipelineConfiguration = MockStages.createPipelineConfigurationComplexSourceProcessorTarget();
    pipeline = new PreviewPipelineBuilder(
      MockStages.createStageLibrary(),
      configuration,
      "name1",
      "0",
      pipelineConfiguration,
      "p6",
      Mockito.mock(LineagePublisherTask.class)
    ).build(MockStages.userContext(), runner);
    previewOutput = pipeline.run();
    output = previewOutput.getBatchesOutput().get(0);
    Assert.assertEquals(3, output.size());

    runner = new PreviewPipelineRunner("name", "0", runtimeInfo, tracker, -1, 1, true, true);
    pipelineConfiguration = MockStages.createPipelineConfigurationComplexSourceProcessorTarget();
    pipeline = new PreviewPipelineBuilder(
      MockStages.createStageLibrary(),
      configuration,
      "name1",
      "0",
      pipelineConfiguration,
      "t",
      Mockito.mock(LineagePublisherTask.class)
    ).build(MockStages.userContext(), runner);
    previewOutput = pipeline.run();
    output = previewOutput.getBatchesOutput().get(0);
    Assert.assertEquals(7, output.size());

  }


  @Test
  public void testIsPreview() throws Exception {
    MockStages.setSourceCapture(new BaseSource() {

      @Override
      protected List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();
        Assert.assertTrue(getContext().isPreview());
        return issues;
      }

      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        return "X";
      }
    });

    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PreviewPipelineRunner runner = new PreviewPipelineRunner("name", "0", runtimeInfo, tracker, -1, 1, true, true);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    pipelineConfiguration.getStages().remove(2);

    PreviewPipeline pipeline = new PreviewPipelineBuilder(
      MockStages.createStageLibrary(),
      configuration,
      "name",
      "0",
      pipelineConfiguration,
      null,
      Mockito.mock(LineagePublisherTask.class)
    ).build(MockStages.userContext(), runner);
    PreviewPipelineOutput previewOutput = pipeline.run();
  }

  @Test
  public void testPreviewRunFailValidationConfigs() throws Exception {

    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> init(Info info, Source.Context context) {
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
    PipelineRunner runner = new PreviewPipelineRunner("name", "0", runtimeInfo, tracker, -1, 1, true, true);
    PreviewPipeline pp = new PreviewPipelineBuilder(
      MockStages.createStageLibrary(),
      configuration,
      "name",
      "0",
      MockStages.createPipelineConfigurationSourceProcessorTarget(),
      null,
      Mockito.mock(LineagePublisherTask.class)
    ).build(MockStages.userContext(), runner);
    Assert.assertFalse(pp.validateConfigs().isEmpty());
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
    PipelineRunner runner = new PreviewPipelineRunner("name", "0", runtimeInfo, tracker, -1, 1, true, true);
    Pipeline pipeline = new MockPipelineBuilder()
      .withConfiguration(configuration)
      .withPipelineConf(pipelineConf)
      .build(runner);
    pipeline.init(false);
    pipeline.run();
    pipeline.destroy(false, PipelineStopReason.UNUSED);
    List<StageOutput> output = runner.getBatchesOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).get().getValue());

    StageOutput sourceOutput = output.get(0);
    Assert.assertEquals("s", sourceOutput.getInstanceName());

    Record modRecord = new RecordImpl("i", "source", null, null);
    modRecord.set(Field.create(10));
    //modifying the source output
    sourceOutput.getOutput().get(pipelineConf.getStages().get(0).getOutputLanes().get(0)).set(0, modRecord);

    runner = new PreviewPipelineRunner("name", "0",runtimeInfo, tracker, -1, 1, true, true);
    pipeline = new MockPipelineBuilder()
      .withConfiguration(configuration)
      .withPipelineConf(pipelineConf)
      .build(runner);

    pipeline.init(false);
    pipeline.run(Arrays.asList(sourceOutput));
    pipeline.destroy(false, PipelineStopReason.UNUSED);
    output = runner.getBatchesOutput().get(0);
    Assert.assertEquals(10, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(20, output.get(1).getOutput().get("p").get(0).get().getValue());
  }

}
