/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.errorrecordstore.impl.FileErrorRecordStore;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFailedProdRun {

  @Before
  public void setUp() {
    MockStages.resetStageCaptures();
  }

  private static final String PIPELINE_NAME = "xyz";
  private static final String REVISION = "0";

  @Test(expected = PipelineRuntimeException.class)
  public void testPipelineOpenLanes() throws PipelineRuntimeException {
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
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(FileSnapshotStore.class),
        Mockito.mock(FileErrorRecordStore.class), 5, 10, 10, DeliveryGuarantee.AT_MOST_ONCE, PIPELINE_NAME, REVISION);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    pipelineConfiguration.getStages().remove(2);

    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(),
        PIPELINE_NAME, pipelineConfiguration).build(runner, tracker);

  }

}
