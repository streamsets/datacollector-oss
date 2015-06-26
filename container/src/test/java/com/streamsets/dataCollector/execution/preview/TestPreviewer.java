/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.preview;

import com.streamsets.dataCollector.execution.PreviewOutput;
import com.streamsets.dataCollector.execution.PreviewStatus;
import com.streamsets.dataCollector.execution.Previewer;
import com.streamsets.dataCollector.execution.PreviewerListener;
import com.streamsets.dataCollector.execution.RawPreview;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineException;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class TestPreviewer {

  protected static final String ID = "myId";
  protected static final String NAME = "myPipeline";
  protected static final String REV = "0";

  protected RuntimeInfo runtimeInfo;
  protected PreviewerListener previewerListener;
  protected Configuration configuration;
  protected StageLibraryTask stageLibrary;
  protected PipelineStoreTask pipelineStore;

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
    runtimeInfo = Mockito.mock(RuntimeInfo.class);
    previewerListener = new RecordingPreviewListener();
    configuration = new Configuration();
    stageLibrary = MockStages.createStageLibrary();
    pipelineStore = Mockito.mock(PipelineStoreTask.class);

  }

  protected abstract Previewer createPreviewer();

  @Test
  public void testValidationConfigsPass() throws Throwable {

    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> validateConfigs(Info info, Source.Context context) {
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

    previewer.validateConfigs();
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

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
    Previewer previewer  = createPreviewer();

    previewer.validateConfigs();
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
      public List<ConfigIssue> validateConfigs(Info info, Source.Context context) throws StageException {
        throw new StageException(MockErrorCode.MOCK_0000);
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
    try {
      previewer.validateConfigs();
      previewer.waitForCompletion(5000);
      Assert.fail("Stage Exception expected");
    } catch (PipelineException pe) {
      Assert.assertTrue(pe.getCause() instanceof StageException);
      StageException e = (StageException) pe.getCause();
      Assert.assertEquals(MockErrorCode.MOCK_0000.getCode(), e.getErrorCode().getCode());
      Assert.assertEquals(MockErrorCode.MOCK_0000.getMessage(), e.getErrorCode().getMessage());
      //If there is an exception while validating then the state remains same
      Assert.assertEquals(PreviewStatus.VALIDATING.name(), previewer.getStatus().name());
      Assert.assertNull(previewer.getOutput());
    }
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
    Assert.assertNull(previewer.getStatus());

    //start preview
    previewer.start(1, 10, false, null, new ArrayList<StageOutput>());
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

    previewer.start(1, 10, true, "p", new ArrayList<StageOutput>());
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    PreviewOutput previewOutput = previewer.getOutput();
    List<StageOutput> output = previewOutput.getOutput().get(0);
    Assert.assertEquals(1, output.size());

    //complex graph
    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationComplexSourceProcessorTarget());
    previewer  = createPreviewer();

    previewer.start(1, 10, true, "p1", new ArrayList<StageOutput>());
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    previewOutput = previewer.getOutput();
    output = previewOutput.getOutput().get(0);
    Assert.assertEquals(1, output.size());
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationComplexSourceProcessorTarget());
    previewer  = createPreviewer();

    previewer.start(1, 10, true, "p5", new ArrayList<StageOutput>());
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    previewOutput = previewer.getOutput();
    output = previewOutput.getOutput().get(0);
    Assert.assertEquals(2, output.size());
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).get().getValue());


    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationComplexSourceProcessorTarget());
    previewer  = createPreviewer();

    previewer.start(1, 10, true, "p6", new ArrayList<StageOutput>());
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.FINISHED.name(), previewer.getStatus().name());
    previewOutput = previewer.getOutput();
    output = previewOutput.getOutput().get(0);
    Assert.assertEquals(3, output.size());

    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationComplexSourceProcessorTarget());
    previewer  = createPreviewer();

    previewer.start(1, 10, true, "t", new ArrayList<StageOutput>());
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

    previewer.start(1, 10, true, null, new ArrayList<StageOutput>());
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

    previewer.start(1, 10, true, null, Arrays.asList(sourceOutput));
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
      public List<ConfigIssue> validateConfigs(Info info, Source.Context context) throws StageException {
        throw new StageException(MockErrorCode.MOCK_0000);
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
    try {
      previewer.start(1, 10, true, null, new ArrayList<StageOutput>());
      previewer.waitForCompletion(5000);
      Assert.fail("Stage Exception expected");
    } catch (PipelineException pe) {
      Assert.assertTrue(pe.getCause() instanceof StageException);
      StageException e = (StageException) pe.getCause();
      Assert.assertEquals(MockErrorCode.MOCK_0000.getCode(), e.getErrorCode().getCode());
      Assert.assertEquals(MockErrorCode.MOCK_0000.getMessage(), e.getErrorCode().getMessage());
      Assert.assertEquals(PreviewStatus.RUN_ERROR.name(), previewer.getStatus().name());
      Assert.assertNull(previewer.getOutput());
    }
  }

  @Test
  public void testPreviewFailValidation() throws Throwable {

    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> validateConfigs(Info info, Source.Context context) throws StageException {
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

    previewer.start(1, 10, true, null, new ArrayList<StageOutput>());
    previewer.waitForCompletion(5000);

    Assert.assertEquals(PreviewStatus.INVALID.name(), previewer.getStatus().name());
    Assert.assertEquals(1, previewer.getOutput().getIssues().getIssueCount());
    Assert.assertEquals(PreviewStatus.INVALID.name(), previewer.getOutput().getStatus().name());
    Assert.assertNull(previewer.getOutput().getOutput());
  }

  @Test
  public void testRawSourcePreview() throws PipelineStoreException, PipelineRuntimeException, IOException {
    Mockito.when(pipelineStore.load(Mockito.anyString(),
      Mockito.anyString())).thenReturn(MockStages.createPipelineConfigurationSourceProcessorTarget());
    Previewer previewer  = createPreviewer();
    MultivaluedMap<String, String> map = new MultivaluedHashMap<>();
    map.putSingle("brokerHost", "localhost");
    map.putSingle("brokerPort", "9001");
    RawPreview rawSource = previewer.getRawSource(100, map);

    Assert.assertEquals("*/*", rawSource.getMimeType());
    Assert.assertEquals("localhost:9001", IOUtils.toString(rawSource.getData()));

  }
}
