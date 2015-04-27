/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import dagger.Module;
import dagger.Provides;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestUtil {

  public static final String MY_PIPELINE = "my pipeline";
  public static final String PIPELINE_REV = "2.0";

  public static class SourceOffsetTrackerImpl implements SourceOffsetTracker {
    private String currentOffset;
    private String newOffset;
    private boolean finished;
    private long lastBatchTime;

    public SourceOffsetTrackerImpl(String currentOffset) {
      this.currentOffset = currentOffset;
      finished = false;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public String getOffset() {
      return currentOffset;
    }

    @Override
    public void setOffset(String newOffset) {
      this.newOffset = newOffset;
    }

    @Override
    public void commitOffset() {
      currentOffset = newOffset;
      finished = (currentOffset == null);
      newOffset = null;
      lastBatchTime = System.currentTimeMillis();
    }

    @Override
    public long getLastBatchTime() {
      return lastBatchTime;
    }
  }


  /********************************************/
  /********* Pipeline using Mock Stages *******/
  /********************************************/

  public static void captureMockStages() {
    MockStages.setSourceCapture(new BaseSource() {
      private int recordsProducedCounter = 0;

      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.set(Field.create(1));
        batchMaker.addRecord(record);
        recordsProducedCounter++;
        if (recordsProducedCounter == 1) {
          recordsProducedCounter = 0;
          return null;
        }
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
  }

  public static void captureStagesForProductionRun() {
    MockStages.setSourceCapture(new BaseSource() {

      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        maxBatchSize = (maxBatchSize > -1) ? maxBatchSize : 10;
        for (int i = 0; i < maxBatchSize; i++ ) {
          batchMaker.addRecord(createRecord(lastSourceOffset, i));
        }
        return "random";
      }

      private Record createRecord(String lastSourceOffset, int batchOffset) {
        Record record = getContext().createRecord("random:" + batchOffset);
        Map<String, Field> map = new HashMap<>();
        map.put("name", Field.create(UUID.randomUUID().toString()));
        map.put("time", Field.create(System.currentTimeMillis()));
        record.set(Field.create(map));
        return record;
      }
    });
    MockStages.setProcessorCapture(new SingleLaneProcessor() {
      private Random random;

      @Override
      protected void init() throws StageException {
        super.init();
        random = new Random();
      }

      @Override
      public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws
          StageException {
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          float action = random.nextFloat();
          getContext().toError(it.next(), "Random error");
        }
        getContext().reportError("Random pipeline error");
      }
    });

    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
      }
    });
  }

  /********************************************/
  /*************** Providers for Dagger *******/
  /********************************************/

  @Module(library = true)
  public static class TestStageLibraryModule {

    public TestStageLibraryModule() {
    }

    @Provides
    public StageLibraryTask provideStageLibrary() {
      return MockStages.createStageLibrary();
    }
  }

  @Module(injects = PipelineStoreTask.class, library = true, includes = {TestRuntimeModule.class, TestConfigurationModule.class})
  public static class TestPipelineStoreModule {

    public TestPipelineStoreModule() {
    }

    @Provides
    public PipelineStoreTask providePipelineStore(RuntimeInfo info, Configuration conf) {
      FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(info, conf);
      pipelineStoreTask.init();
      try {
        //create an invalid pipeline
        pipelineStoreTask.create("invalid", "invalid cox its empty", "tag");

        //create a valid pipeline
        pipelineStoreTask.create(MY_PIPELINE, "description", "tag");
        PipelineConfiguration pipelineConf = pipelineStoreTask.load(MY_PIPELINE, PIPELINE_REV);
        PipelineConfiguration mockPipelineConf = MockStages.createPipelineConfigurationSourceProcessorTarget();
        pipelineConf.setStages(mockPipelineConf.getStages());
        pipelineConf.setErrorStage(mockPipelineConf.getErrorStage());
        pipelineStoreTask.save(MY_PIPELINE, "admin", "tag", "description"
          , pipelineConf);

        //create a DataRuleDefinition for one of the stages
        DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("myID", "myLabel", "s", 100, 10,
          "${record:value(\"/name\") != null}", true, "alertText", ThresholdType.COUNT, "100", 100, true, false, true);
        List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
        dataRuleDefinitions.add(dataRuleDefinition);

        RuleDefinitions ruleDefinitions = new RuleDefinitions(Collections.<MetricsRuleDefinition>emptyList(),
          dataRuleDefinitions, Collections.<String>emptyList(), UUID.randomUUID());
        pipelineStoreTask.storeRules(MY_PIPELINE, PIPELINE_REV, ruleDefinitions);

      } catch (PipelineStoreException e) {
        throw new RuntimeException(e);
      }

      return pipelineStoreTask;
    }
  }

  @Module(library = true)
  public static class TestConfigurationModule {

    public TestConfigurationModule() {
    }

    @Provides
    public Configuration provideRuntimeInfo() {
      Configuration conf = new Configuration();
      return conf;
    }
  }

  @Module(library = true)
  public static class TestRuntimeModule {

    public TestRuntimeModule() {
    }

    @Provides
    public RuntimeInfo provideRuntimeInfo() {
      RuntimeInfo info = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(getClass().getClassLoader()));
      return info;
    }
  }

  @Module(injects = ProductionPipelineManagerTask.class
    , library = true, includes = {TestRuntimeModule.class, TestPipelineStoreModule.class
    , TestStageLibraryModule.class, TestConfigurationModule.class})
  public static class TestProdManagerModule {

    public TestProdManagerModule() {
    }

    @Provides
    public ProductionPipelineManagerTask provideStateManager(RuntimeInfo RuntimeInfo, Configuration configuration
      ,PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary) {
      return new ProductionPipelineManagerTask(RuntimeInfo, configuration, pipelineStore, stageLibrary);
    }
  }

  /********************************************/
  /*************** Utility methods ************/
  /********************************************/

  public static void stopPipelineIfNeeded(ProductionPipelineManagerTask manager) throws InterruptedException, PipelineManagerException {
    if(manager.getPipelineState().getState() == State.RUNNING) {
      manager.stopPipeline(false);
    }
    long start = System.currentTimeMillis();
    while(manager.getPipelineState().getState() != State.FINISHED &&
      manager.getPipelineState().getState() != State.STOPPED &&
      manager.getPipelineState().getState() != State.ERROR) {
      Thread.sleep(5);
      long elapsed = System.currentTimeMillis() - start;
      if (elapsed > TimeUnit.MINUTES.toMillis(5)) {
        String msg = "TimedOut waiting for pipeline to stop. State is currently: " +
          manager.getPipelineState().getState() + " after " + TimeUnit.MILLISECONDS.toMinutes(elapsed) + "min";
        throw new IllegalStateException(msg);
      }
    }
  }

}
