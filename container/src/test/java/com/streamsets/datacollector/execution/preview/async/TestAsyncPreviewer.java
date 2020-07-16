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
package com.streamsets.datacollector.execution.preview.async;

import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.preview.TestPreviewer;
import com.streamsets.datacollector.execution.preview.sync.SyncPreviewer;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.util.PipelineException;
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
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.streamsets.datacollector.util.AwaitConditionUtil.desiredPreviewStatus;
import static org.awaitility.Awaitility.await;

public class TestAsyncPreviewer extends TestPreviewer {

  protected Previewer createPreviewer() {
    return new AsyncPreviewer(new SyncPreviewer(
        ID,
        "test-user",
        NAME,
        REV,
        previewerListener,
        objectGraph,
        Collections.emptyList(),
        p -> null,
        new HashMap<>()
    ),
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
    final Previewer previewer  = createPreviewer();
    previewer.validateConfigs(100);
    await().until(desiredPreviewStatus(previewer, PreviewStatus.TIMED_OUT));
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
    final Previewer previewer  = createPreviewer();
    previewer.start(1, 10, false, false,null,
        new ArrayList<StageOutput>(), 200, false);

    await().until(desiredPreviewStatus(previewer, PreviewStatus.TIMED_OUT));
  }

}
