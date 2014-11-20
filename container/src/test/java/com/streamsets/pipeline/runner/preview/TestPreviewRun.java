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
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.StageOutput;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;


public class TestPreviewRun {

  @Test
  public void testPreviewRun() throws Exception {
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.setField("f", Field.create(1));
        batchMaker.addRecord(record);
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.setField("f", Field.create(2));
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
    List<StageOutput> output = runner.getBatchesOutput();
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).getField("f").getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).getField("f").getValue());
  }

  @Test
  public void testPreviewPipelineBuilder() throws Exception {
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.setField("f", Field.create(1));
        batchMaker.addRecord(record);
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.setField("f", Field.create(2));
        batchMaker.addRecord(record);
      }
    });
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(tracker, -1, 1, true);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    pipelineConfiguration.getStages().remove(2);

    PreviewPipeline pipeline = new PreviewPipelineBuilder(MockStages.createStageLibrary(), "name", pipelineConfiguration).build(runner);
    PreviewPipelineOutput previewOutput = pipeline.run();
    List<StageOutput> output = previewOutput.getBatchesOutput();
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).getField("f").getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).getField("f").getValue());
  }

}
