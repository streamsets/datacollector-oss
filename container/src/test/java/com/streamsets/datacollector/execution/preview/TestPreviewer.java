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
package com.streamsets.datacollector.execution.preview;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.execution.preview.sync.SyncPreviewer;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class TestPreviewer {

  protected static final String ID = "myId";
  protected static final String NAME = "myPipeline";
  protected static final String REV = "0";

  protected RuntimeInfo runtimeInfo;
  protected PreviewerListener previewerListener;
  protected Configuration configuration;
  protected StageLibraryTask stageLibrary;
  protected PipelineStoreTask pipelineStore;
  protected ObjectGraph objectGraph;

  @Module(
    injects = {
      RuntimeInfo.class,
      BuildInfo.class,
      Configuration.class,
      StageLibraryTask.class,
      PipelineStoreTask.class,
      SyncPreviewer.class,
      BlobStoreTask.class,
      LineagePublisherTask.class,
      StatsCollector.class
    },
    library = true
  )
  static class TestPreviewModule {
    @Provides
    @Singleton
    public RuntimeInfo providesRuntimeInfo() {
      return new StandaloneRuntimeInfo(
          RuntimeInfo.SDC_PRODUCT,
          RuntimeModule.SDC_PROPERTY_PREFIX,
          new MetricRegistry(),
          Arrays.asList(TestPreviewer.class.getClassLoader())
      );
    }

    @Provides
    @Singleton
    public BuildInfo providesBuildInfo() {
      BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
      Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
      return buildInfo;
    }

    @Provides @Singleton
    public Configuration provideConfiguration() {
      Configuration configuration = new Configuration();
      return configuration;
    }

    @Provides @Singleton
    public PipelineStoreTask providePipelineStoreTask() {
      return Mockito.mock(PipelineStoreTask.class);
    }

    @Provides @Singleton
    public StageLibraryTask provideStageLibraryTask() {
      return MockStages.createStageLibrary(new URLClassLoader(new URL[0]));
    }

    @Provides @Singleton
    public StatsCollector provideStatsCollector() {
      return Mockito.mock(StatsCollector.class);
    }

    @Provides
    @Singleton
    public LineagePublisherTask providesLineagePublisherTask() {
      return Mockito.mock(LineagePublisherTask.class);
    }

    @Provides
    @Singleton
    public BlobStoreTask providesBlobStoreTask() {
      return Mockito.mock(BlobStoreTask.class);
    }

  }

  //Mock Error Code implementation
  enum MockErrorCode implements ErrorCode {
    MOCK_0000
    ;

    @Override
    public String getCode() {
      return "MOCK_0000";
    }

    @Override
    public String getMessage() {
      return "Mock Error";
    }
  }

  static class RecordingPreviewListener implements PreviewerListener {

    private List<PreviewStatus> previewStatuses;

    public RecordingPreviewListener() {
      this.previewStatuses = new ArrayList<>();
    }

    @Override
    public void statusChange(String id, PreviewStatus status) {
      previewStatuses.add(status);
    }

    @Override
    public void outputRetrieved(String id) {

    }

    public List<PreviewStatus> getPreviewStatuses() {
      return previewStatuses;
    }
  }

  @Before
  public void setUp() throws PipelineStoreException {
    MockStages.resetStageCaptures();
    previewerListener = new RecordingPreviewListener();

    objectGraph = ObjectGraph.create(TestPreviewModule.class);
    runtimeInfo = objectGraph.get(RuntimeInfo.class);
    configuration = objectGraph.get(Configuration.class);
    stageLibrary = objectGraph.get(StageLibraryTask.class);
    pipelineStore = objectGraph.get(PipelineStoreTask.class);

  }

  protected abstract Previewer createPreviewer();

  @Test
  public void testValidationConfigsPass() throws Throwable {

    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> init(Info info, Source.Context context) {
        return Collections.emptyList();
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

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());

    Previewer previewer  = createPreviewer();

    previewer.validateConfigs(5000);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.VALID.name(), previewer.getStatus().name());
    Assert.assertEquals(0, previewer.getOutput().getIssues().getIssueCount());
    Assert.assertEquals(PreviewStatus.VALID.name(), previewer.getOutput().getStatus().name());
    Assert.assertNull(previewer.getOutput().getOutput());

    List<PreviewStatus> previewStatuses = ((RecordingPreviewListener) previewerListener).getPreviewStatuses();
    Assert.assertEquals(2, previewStatuses.size());
    Assert.assertEquals(PreviewStatus.VALIDATING.name(), previewStatuses.get(0).name());
    Assert.assertEquals(PreviewStatus.VALID.name(), previewStatuses.get(1).name());
  }

  @Test
  public void testValidationConfigsFail() throws Throwable {

    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
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

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
    Previewer previewer  = createPreviewer();

    previewer.validateConfigs(5000);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.INVALID.name(), previewer.getStatus().name());
    Assert.assertEquals(1, previewer.getOutput().getIssues().getIssueCount());
    Assert.assertEquals(PreviewStatus.INVALID.name(), previewer.getOutput().getStatus().name());
    Assert.assertNull(previewer.getOutput().getOutput());
  }

  @Test
  public void testValidationConfigsException() throws Throwable {

    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> init(Info info, Source.Context context) {
        return ImmutableList.of(context.createConfigIssue(null, null, MockErrorCode.MOCK_0000));
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

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
    Previewer previewer  = createPreviewer();
    previewer.validateConfigs(5000);
    previewer.waitForCompletion(5000);
    Assert.assertEquals(PreviewStatus.INVALID, previewer.getStatus());
    Assert.assertTrue(previewer.getOutput().getIssues().getIssueCount() > 0);
  }

  @Test
  public void testPreviewRun() throws Throwable {
    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());

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

    //create Sync Previewer
    Previewer previewer  = createPreviewer();

    //check id, name, revision
    Assert.assertEquals(ID, previewer.getId());
    Assert.assertEquals(NAME, previewer.getName());
    Assert.assertEquals(REV, previewer.getRev());
    Assert.assertEquals(PreviewStatus.CREATED, previewer.getStatus());

    //start preview
    previewer.start(1, 10, false, true, null,
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    //when sync previewer returns from start, the preview should be finished
    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());

    //stop should be a no-op
    previewer.stop();
    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());

    //check the output
    PreviewOutput previewOutput = previewer.getOutput();
    List<StageOutput> output = previewOutput.getOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).get().getValue());

    List<PreviewStatus> previewStatuses = ((RecordingPreviewListener) previewerListener).getPreviewStatuses();
    Assert.assertEquals(2, previewStatuses.size());
    Assert.assertEquals(PreviewStatus.RUNNING.name(), previewStatuses.get(0).name());
    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewStatuses.get(1).name());
  }

  @Test
  public void testPreviewRunPushSource() throws Throwable {
    Mockito
      .when(pipelineStore.load(Mockito.anyString(), Mockito.anyString()))
      .thenReturn(MockStages.createPipelineConfigurationPushSourceTarget());

    MockStages.setPushSourceCapture(new BasePushSource() {
      @Override
      public int getNumberOfThreads() {
        return 1;
      }

      @Override
      public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
        BatchContext batchContext = getContext().startBatch();

        Record record = getContext().createRecord("x");
        record.set(Field.create(1));
        batchContext.getBatchMaker().addRecord(record);

        getContext().processBatch(batchContext);
      }
    });

    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
      }
    });

    //create Sync Previewer
    Previewer previewer  = createPreviewer();

    //check id, name, revision
    Assert.assertEquals(ID, previewer.getId());
    Assert.assertEquals(NAME, previewer.getName());
    Assert.assertEquals(REV, previewer.getRev());
    Assert.assertEquals(PreviewStatus.CREATED, previewer.getStatus());

    //start preview
    previewer.start(1, 10, false, true, null,
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    //when sync previewer returns from start, the preview should be finished
    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());

    //stop should be a no-op
    previewer.stop();
    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());

    //check the output
    PreviewOutput previewOutput = previewer.getOutput();
    List<StageOutput> output = previewOutput.getOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("a").get(0).get().getValue());

    List<PreviewStatus> previewStatuses = ((RecordingPreviewListener) previewerListener).getPreviewStatuses();
    Assert.assertEquals(2, previewStatuses.size());
    Assert.assertEquals(PreviewStatus.RUNNING.name(), previewStatuses.get(0).name());
    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewStatuses.get(1).name());
  }

  @Test
  public void testPreviewPipelineBuilderWithLastStage() throws Throwable {
    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
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

    Previewer previewer  = createPreviewer();

    previewer.start(1, 10, true, true, "p",
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    PreviewOutput previewOutput = previewer.getOutput();
    List<StageOutput> output = previewOutput.getOutput().get(0);
    Assert.assertEquals(1, output.size());

    //complex graph
    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationComplexSourceProcessorTarget());
    previewer  = createPreviewer();

    previewer.start(1, 10, true, true,"p1",
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    previewOutput = previewer.getOutput();
    output = previewOutput.getOutput().get(0);
    Assert.assertEquals(1, output.size());
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationComplexSourceProcessorTarget());
    previewer  = createPreviewer();

    previewer.start(1, 10, true, true,"p5",
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    previewOutput = previewer.getOutput();
    output = previewOutput.getOutput().get(0);
    Assert.assertEquals(2, output.size());
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());


    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationComplexSourceProcessorTarget());
    previewer  = createPreviewer();

    previewer.start(1, 10, true, true,"p6",
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    previewOutput = previewer.getOutput();
    output = previewOutput.getOutput().get(0);
    Assert.assertEquals(3, output.size());

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationComplexSourceProcessorTarget());
    previewer  = createPreviewer();

    previewer.start(1, 10, true, true,"t",
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    previewOutput = previewer.getOutput();
    output = previewOutput.getOutput().get(0);
    Assert.assertEquals(7, output.size());

  }

  @Test
  public void testPreviewRunOverride() throws Throwable {
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
    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
    PipelineConfiguration pipelineConf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    Previewer previewer  = createPreviewer();

    previewer.start(1, 10, true, true,null,
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    PreviewOutput previewOutput = previewer.getOutput();
    List<StageOutput> output = previewOutput.getOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).get().getValue());

    StageOutput sourceOutput = output.get(0);
    Assert.assertEquals("s", sourceOutput.getInstanceName());

    Record modRecord = new RecordImpl("i", "source", null, null);
    modRecord.set(Field.create(10));
    //modifying the source output
    sourceOutput.getOutput().get(pipelineConf.getStages().get(0).getOutputLanes().get(0)).set(0, modRecord);

    previewer.start(1, 10, true, true,null,
        Arrays.asList(sourceOutput), 5000, false);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    previewOutput = previewer.getOutput();
    output = previewOutput.getOutput().get(0);
    Assert.assertEquals(10, output.get(0).getOutput().get("s").get(0).get().getValue());
    Assert.assertEquals(20, output.get(1).getOutput().get("p").get(0).get().getValue());
  }

  @Test
  public void testPreviewException() throws Throwable {

    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> init(Info info, Source.Context context) {
        throw new RuntimeException();
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

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
    Previewer previewer  = createPreviewer();
    previewer.start(1, 10, true, true,null,
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);
    Assert.assertEquals(PreviewStatus.INVALID, previewer.getStatus());
    Assert.assertTrue(previewer.getOutput().getIssues().getIssueCount() > 0);
  }

  @Test
  public void testPreviewFailValidation() throws Throwable {

    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
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

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
    Previewer previewer  = createPreviewer();

    previewer.start(1, 10, true, true,null,
        new ArrayList<StageOutput>(), 5000, false);
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.INVALID.name(), previewer.getStatus().name());
    Assert.assertEquals(1, previewer.getOutput().getIssues().getIssueCount());
    Assert.assertEquals(PreviewStatus.INVALID.name(), previewer.getOutput().getStatus().name());
    Assert.assertNull(previewer.getOutput().getOutput());
  }

  @Test
  public void testRawSourcePreview() throws PipelineException, IOException {
    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
    Previewer previewer  = createPreviewer();
    MultivaluedMap<String, String> map = new MultivaluedHashMap<>();
    map.putSingle("brokerHost", "localhost");
    map.putSingle("brokerPort", "9001");
    RawPreview rawSource = previewer.getRawSource(100, map);

    Assert.assertEquals("*/*", rawSource.getMimeType());
    Assert.assertEquals("localhost:9001", rawSource.getPreviewData());

  }
}
