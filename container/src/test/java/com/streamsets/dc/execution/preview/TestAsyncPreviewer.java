/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.preview;

import com.streamsets.dc.execution.PreviewStatus;
import com.streamsets.dc.execution.Previewer;
import com.streamsets.dc.execution.preview.async.AsyncPreviewer;
import com.streamsets.dc.execution.preview.sync.SyncPreviewer;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.util.PipelineException;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class TestAsyncPreviewer extends TestPreviewer {

  protected Previewer createPreviewer() {
    return new AsyncPreviewer(new SyncPreviewer(ID, NAME, REV, previewerListener, objectGraph),
      new SafeScheduledExecutorService(5, "preview"));
  }

  @Test(timeout = 5000)
  public void testValidateConfigsTimeout() throws PipelineException, InterruptedException {
    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> init(Info info, Source.Context context) {
        while(true);
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
    previewer.validateConfigs(100);
    while(previewer.getStatus() != PreviewStatus.TIMED_OUT) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 5000)
  public void testStartTimeout() throws PipelineException, InterruptedException {
    //Source validateConfigs method is overridden to create a config issue with error code CONTAINER_0000
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public List<ConfigIssue> init(Info info, Source.Context context) {
        while(true);
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
    previewer.start(1, 10, false, null, new ArrayList<StageOutput>(), 200);
    while(previewer.getStatus() != PreviewStatus.TIMED_OUT) {
      Thread.sleep(100);
    }
  }
}
