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
package com.streamsets.datacollector.util;

import com.codahale.metrics.MetricRegistry;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetup;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.alerts.AlertManager;
import com.streamsets.datacollector.execution.manager.PreviewerProvider;
import com.streamsets.datacollector.execution.manager.RunnerProvider;
import com.streamsets.datacollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.datacollector.execution.runner.common.AsyncRunner;
import com.streamsets.datacollector.execution.runner.common.DataObserverRunnable;
import com.streamsets.datacollector.execution.runner.common.MetricObserverRunnable;
import com.streamsets.datacollector.execution.runner.common.MetricsObserverRunner;
import com.streamsets.datacollector.execution.runner.common.ProductionObserver;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineRunner;
import com.streamsets.datacollector.execution.runner.common.RulesConfigLoader;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.datacollector.execution.store.CachePipelineStateStore;
import com.streamsets.datacollector.execution.store.FilePipelineStateStore;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.datacollector.runner.production.RulesConfigLoaderRunnable;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.FileAclStoreTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.mockito.Mockito;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class TestUtil {
  public static final String USER = "user";
  public static final String MY_PIPELINE = "my pipeline";
  public static final String MY_SECOND_PIPELINE = "my second pipeline";
  public static final String HIGHER_VERSION_PIPELINE = "higher version pipeline";
  public static final String PIPELINE_WITH_EMAIL = "Pipeline with Email";
  public static final String PIPELINE_TITLE_WITH_EMAIL = "label";
  public static final String PIPELINE_REV = "2.0";
  public static final String ZERO_REV = "0";
  public volatile static boolean EMPTY_OFFSET = false;

  public static class SourceOffsetTrackerImpl implements SourceOffsetTracker {
    private final Map<String, String> offsets;
    private boolean finished;
    private long lastBatchTime;

    public SourceOffsetTrackerImpl(Map<String, String> offsets) {
      this.offsets = new HashMap<>(offsets);
      finished = false;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public void commitOffset(String entity, String newOffset) {
      lastBatchTime = System.currentTimeMillis();
      System.out.println(Utils.format("Committing entity({}), offset({}) on time({})", entity, newOffset, lastBatchTime));

      if(entity == null) {
        return;
      }

      if(Source.POLL_SOURCE_OFFSET_KEY.equals(entity)) {
        finished = (newOffset == null);
      }

      if(newOffset == null) {
        offsets.remove(entity);
      } else {
        offsets.put(entity, newOffset);
      }
    }

    @Override
    public Map<String, String> getOffsets() {
      return offsets;
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
        return EMPTY_OFFSET  == true ? null: "random";
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
      protected List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();
        random = new Random();
        return issues;
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

  public static void captureMockStagesLongWait() {
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
        try {
          Thread.sleep(1000000);
        } catch (InterruptedException e) {
          // No-op
        }
        batchMaker.addRecord(record);
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


  /*************** StageLibrary ***************/

  @Module(library = true)
  public static class TestStageLibraryModule {

    public TestStageLibraryModule() {
    }

    @Provides @Singleton
    public StageLibraryTask provideStageLibrary() {
      return MockStages.createStageLibrary();
    }
  }

  /*************** Lineage ***************/
  @Module(
    injects = {
      LineagePublisherTask.class
    },
    library = true
  )
  public static class TestLineageModule {
    @Provides
    @Singleton
    public LineagePublisherTask provideLineagePublisher() {
      return Mockito.mock(LineagePublisherTask.class);
    }
  }

  /*************** PipelineStore ***************/
  // TODO - Rename TestPipelineStoreModule after multi pipeline support
  @Module(
      injects = {PipelineStoreTask.class, Configuration.class},
      library = true,
      includes = {TestRuntimeModule.class, TestStageLibraryModule.class,  TestPipelineStateStoreModule.class }
  )
  public static class TestPipelineStoreModuleNew {

    public TestPipelineStoreModuleNew() {

    }

    @Provides @Singleton
    public PipelineStoreTask providePipelineStore(RuntimeInfo info, StageLibraryTask stageLibraryTask, PipelineStateStore pipelineStateStore) {
      FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(info, stageLibraryTask, pipelineStateStore, new LockCache<String>());
      pipelineStoreTask.init();
      try {
        //create an invalid pipeline
        //The if check is needed because the tests restart the pipeline manager. In that case the check prevents
        //us from trying to create the same pipeline again
        if(!pipelineStoreTask.hasPipeline("invalid")) {
          pipelineStoreTask.create(USER, "invalid", "label" ,"invalid its empty", false);
          PipelineConfiguration pipelineConf = pipelineStoreTask.load("invalid", PIPELINE_REV);
          PipelineConfiguration mockPipelineConf = MockStages.createPipelineConfigurationSourceTarget();
          pipelineConf.setErrorStage(mockPipelineConf.getErrorStage());
          pipelineConf.getConfiguration().add(new Config("executionMode",
            ExecutionMode.STANDALONE.name()));
        }

        if (!pipelineStoreTask.hasPipeline(MY_PIPELINE)) {
          pipelineStoreTask.create(USER, MY_PIPELINE, "label" ,"description", false);
          PipelineConfiguration pipelineConf = pipelineStoreTask.load(MY_PIPELINE, ZERO_REV);
          PipelineConfiguration mockPipelineConf = MockStages.createPipelineConfigurationSourceTarget();
          pipelineConf.setStages(mockPipelineConf.getStages());
          pipelineConf.setErrorStage(mockPipelineConf.getErrorStage());
          pipelineConf.setStatsAggregatorStage(mockPipelineConf.getStatsAggregatorStage());
          pipelineConf.getConfiguration().add(new Config("executionMode", ExecutionMode.STANDALONE.name()));
          pipelineConf.getConfiguration().add(new Config("retryAttempts", 3));
          pipelineStoreTask.save("admin", MY_PIPELINE, ZERO_REV, "description", pipelineConf);

          // create a DataRuleDefinition for one of the stages
          DataRuleDefinition dataRuleDefinition =
            new DataRuleDefinition("myID", "myLabel", "s", 100, 10, "${record:value(\"/name\") != null}", true,
              "alertText", ThresholdType.COUNT, "100", 100, true, false, true, System.currentTimeMillis());
          List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
          dataRuleDefinitions.add(dataRuleDefinition);

          RuleDefinitions ruleDefinitions =
            new RuleDefinitions(
                PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
                RuleDefinitionsConfigBean.VERSION,
                Collections.<MetricsRuleDefinition> emptyList(),
                dataRuleDefinitions,
                Collections.<DriftRuleDefinition>emptyList(),
                Collections.<String> emptyList(),
                UUID.randomUUID(),
                Collections.emptyList()
            );
          pipelineStoreTask.storeRules(MY_PIPELINE, ZERO_REV, ruleDefinitions);
        }

        if(!pipelineStoreTask.hasPipeline(MY_SECOND_PIPELINE)) {
          pipelineStoreTask.create("user2", MY_SECOND_PIPELINE, "label" ,"description2", false);
          PipelineConfiguration pipelineConf = pipelineStoreTask.load(MY_SECOND_PIPELINE, ZERO_REV);
          PipelineConfiguration mockPipelineConf = MockStages.createPipelineConfigurationSourceProcessorTarget();
          pipelineConf.setStages(mockPipelineConf.getStages());
          pipelineConf.setErrorStage(mockPipelineConf.getErrorStage());
          pipelineConf.setStatsAggregatorStage(mockPipelineConf.getStatsAggregatorStage());
          pipelineConf.getConfiguration().add(new Config("executionMode",
            ExecutionMode.STANDALONE.name()));
          pipelineStoreTask.save("admin2", MY_SECOND_PIPELINE, ZERO_REV, "description"
            , pipelineConf);
        }

        if(!pipelineStoreTask.hasPipeline(HIGHER_VERSION_PIPELINE)) {
          PipelineConfiguration pipelineConfiguration = pipelineStoreTask.create("user2", HIGHER_VERSION_PIPELINE,
              "label" ,"description2", false);
          PipelineConfiguration mockPipelineConf = MockStages.createPipelineConfigurationSourceProcessorTargetHigherVersion();
          mockPipelineConf.getConfiguration().add(new Config("executionMode",
            ExecutionMode.STANDALONE.name()));
          mockPipelineConf.setUuid(pipelineConfiguration.getUuid());
          pipelineStoreTask.save("admin2", HIGHER_VERSION_PIPELINE, ZERO_REV, "description"
            , mockPipelineConf);
        }

        if(!pipelineStoreTask.hasPipeline(PIPELINE_WITH_EMAIL)) {
          pipelineStoreTask.create("user2", PIPELINE_WITH_EMAIL, "label" ,"description2", false);
          PipelineConfiguration pipelineConf = pipelineStoreTask.load(PIPELINE_WITH_EMAIL, ZERO_REV);
          PipelineConfiguration mockPipelineConf = MockStages.createPipelineConfigurationSourceProcessorTarget();
          pipelineConf.setStages(mockPipelineConf.getStages());
          pipelineConf.setErrorStage(mockPipelineConf.getErrorStage());
          pipelineConf.setStatsAggregatorStage(mockPipelineConf.getStatsAggregatorStage());
          pipelineConf.getConfiguration().add(new Config("executionMode",
            ExecutionMode.STANDALONE.name()));
          pipelineConf.getConfiguration().add(new Config("notifyOnTermination", true));
          pipelineConf.getConfiguration().add(new Config("emailIDs", Arrays.asList("foo", "bar")));
          pipelineStoreTask.save("admin2", PIPELINE_WITH_EMAIL, ZERO_REV, "description"
            , pipelineConf);
        }

      } catch (PipelineStoreException e) {
        throw new RuntimeException(e);
      }

      return pipelineStoreTask;
    }
  }

  /*************** PipelineStateStore ***************/

  @Module(injects = PipelineStateStore.class, library = true, includes = {TestRuntimeModule.class})
  public static class TestPipelineStateStoreModule {
    public TestPipelineStateStoreModule() {
    }

    @Provides @Singleton
    public PipelineStateStore providePipelineStore(RuntimeInfo info, Configuration conf) {
      PipelineStateStore pipelineStateStore = new FilePipelineStateStore(info, conf);
      CachePipelineStateStore cachePipelineStateStore = new CachePipelineStateStore(pipelineStateStore, conf);
      cachePipelineStateStore.init();
      return cachePipelineStateStore;
    }
  }

  @Module(
      injects = {AclStoreTask.class},
      library = true,
      includes = {TestRuntimeModule.class, TestPipelineStoreModuleNew.class}
  )
  public static class TestAclStoreModule {
    public TestAclStoreModule() {
    }

    @Provides @Singleton
    public AclStoreTask provideAclStore(RuntimeInfo info, PipelineStoreTask pipelineStoreTask) {
      AclStoreTask aclStoreTask = new FileAclStoreTask(info, pipelineStoreTask,  new LockCache<String>(),
          Mockito.mock(UserGroupManager.class));
      aclStoreTask.init();
      return aclStoreTask;
    }
  }

  /*************** RuntimeInfo ***************/

  @Module(library = true)
  public static class TestRuntimeModule {

    private static GreenMail server;

    public static GreenMail getMailServer() {
      if(server == null) {
        int port = 25;
        try {
          ServerSocket serverSocket = new ServerSocket(0);
          port = serverSocket.getLocalPort();
          serverSocket.close();
        } catch (IOException e) {

        }
        ServerSetup serverSetup = new ServerSetup(port, "localhost", "smtp");
        server = new GreenMail(serverSetup);
        server.setUser("user@x", "user", "password");
        server.start();
      }
      return server;
    }

    public TestRuntimeModule() {
    }

    @Provides
    @Singleton
    public Configuration provideConfiguration() {
      Configuration conf = new Configuration();
      conf.set("mail.smtp.host", "localhost");
      conf.set("mail.smtp.port", getMailServer().getSmtp().getPort());
      return conf;
    }

    @Provides @Singleton
    public RuntimeInfo provideRuntimeInfo() {
      RuntimeInfo info = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(getClass().getClassLoader()));
      return info;
    }



    @Provides @Singleton
    public EventListenerManager provideEventListenerManager() {
      return new EventListenerManager();
    }


  }

  /*************** SafeScheduledExecutorService ***************/

  @Module(library = true)
  public static class TestExecutorModule {

    @Provides @Named("previewExecutor")
    public SafeScheduledExecutorService providePreviewExecutor() {
      return new SafeScheduledExecutorService(1, "preview");
    }

    @Provides @Named("runnerExecutor") @Singleton
    public SafeScheduledExecutorService provideRunnerExecutor() {
      return new SafeScheduledExecutorService(10, "runner");
    }

    @Provides @Named("runnerStopExecutor") @Singleton
    public SafeScheduledExecutorService provideRunnerStopExecutor() {
      return new SafeScheduledExecutorService(10, "runnerStop");
    }

    @Provides @Named("managerExecutor") @Singleton
    public SafeScheduledExecutorService provideManagerExecutor() {
      return new SafeScheduledExecutorService(10, "manager");

    }
  }

  /*************** PipelineProvider ***************/

  @Module(
    injects = {
      EmailSender.class,
      AlertManager.class,
      Observer.class,
      RulesConfigLoader.class,
      ThreadHealthReporter.class,
      DataObserverRunnable.class,
      RulesConfigLoaderRunnable.class,
      MetricObserverRunnable.class,
      SourceOffsetTracker.class,
      PipelineRunner.class,
      com.streamsets.datacollector.execution.runner.common.ProductionPipelineBuilder.class
    },
    library = true,
    includes = {
      TestRuntimeModule.class,
      TestPipelineStoreModuleNew.class,
      TestSnapshotStoreModule.class,
      TestLineageModule.class
    })
  public static class TestPipelineProviderModule {

    private String name;
    private String rev;


    public TestPipelineProviderModule() {
    }

    public TestPipelineProviderModule(String name, String rev) {
      this.name = name;
      this.rev = rev;
    }

    @Provides
    @Named("name")
    public String provideName() {
      return name;
    }

    @Provides
    @Named("rev")
    public String provideRev() {
      return rev;
    }

    @Provides @Singleton
    public MetricRegistry provideMetricRegistry() {
      return new MetricRegistry();
    }

    @Provides @Singleton
    public EmailSender provideEmailSender() {
      return Mockito.mock(EmailSender.class);
    }

    @Provides @Singleton
    public AlertManager provideAlertManager() {
      return Mockito.mock(AlertManager.class);
    }

    @Provides @Singleton
    public MetricsObserverRunner provideMetricsObserverRunner() {
      return Mockito.mock(MetricsObserverRunner.class);
    }

    @Provides @Singleton
    public Observer provProductionObserver() {
      return Mockito.mock(ProductionObserver.class);
    }

    @Provides @Singleton
    public RulesConfigLoader provideRulesConfigLoader() {
      return Mockito.mock(RulesConfigLoader.class);
    }

    @Provides @Singleton
    public ThreadHealthReporter provideThreadHealthReporter() {
      return Mockito.mock(ThreadHealthReporter.class);
    }

    @Provides @Singleton
    public RulesConfigLoaderRunnable provideRulesConfigLoaderRunnable() {
      return Mockito.mock(RulesConfigLoaderRunnable.class);
    }

    @Provides @Singleton
    public MetricObserverRunnable provideMetricObserverRunnable() {
      return Mockito.mock(MetricObserverRunnable.class);
    }

    @Provides @Singleton
    public DataObserverRunnable provideDataObserverRunnable() {
      return Mockito.mock(DataObserverRunnable.class);
    }

    @Provides @Singleton
    public SourceOffsetTracker provideProductionSourceOffsetTracker(@Named("name") String name,
                                                                              @Named("rev") String rev,
                                                                              RuntimeInfo runtimeInfo) {
      return new ProductionSourceOffsetTracker(name, rev, runtimeInfo);
    }

    @Provides @Singleton
    public PipelineRunner provideProductionPipelineRunner(@Named("name") String name,
                                                                    @Named("rev") String rev, Configuration configuration, RuntimeInfo runtimeInfo,
                                                                    MetricRegistry metrics, SnapshotStore snapshotStore,
                                                                    ThreadHealthReporter threadHealthReporter,
                                                                    SourceOffsetTracker sourceOffsetTracker) {
      return new com.streamsets.datacollector.execution.runner.common.ProductionPipelineRunner(name, rev, configuration, runtimeInfo, metrics, snapshotStore,
        threadHealthReporter);
    }

    @Provides @Singleton
    public com.streamsets.datacollector.execution.runner.common.ProductionPipelineBuilder provideProductionPipelineBuilder(@Named("name") String name,
                                                                      @Named("rev") String rev,
                                                                      RuntimeInfo runtimeInfo, StageLibraryTask stageLib,
                                                                      PipelineRunner runner, Observer observer) {
      return new com.streamsets.datacollector.execution.runner.common.ProductionPipelineBuilder(
        name,
        rev,
        new Configuration(),
        runtimeInfo,
        stageLib,
        (ProductionPipelineRunner)runner,
        observer,
        Mockito.mock(LineagePublisherTask.class)
      );
    }
  }

  /*************** Runner ***************/

  @Module(injects = Runner.class, library = true, includes = {TestExecutorModule.class, TestPipelineStoreModuleNew.class,
    TestPipelineStateStoreModule.class, TestPipelineProviderModule.class})
  public static class TestRunnerModule {

    private final String name;
    private final String rev;
    private final ObjectGraph objectGraph;

    public TestRunnerModule(String name, String rev, ObjectGraph objectGraph) {
      this.name = name;
      this.rev = rev;
      this.objectGraph = objectGraph;
    }

    @Provides
    public Runner provideRunner(
      @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor,
      @Named("runnerStopExecutor") SafeScheduledExecutorService runnerStopExecutor
    ) {
      return new AsyncRunner(new StandaloneRunner(name, rev, objectGraph), runnerExecutor, runnerStopExecutor);
    }
  }

  /*************** SnapshotStore ***************/

  @Module(injects = SnapshotStore.class, library = true, includes = {TestRuntimeModule.class,
    LockCacheModule.class})
  public static class TestSnapshotStoreModule {
    @Provides
    @Singleton
    public SnapshotStore provideSnapshotStore(RuntimeInfo runtimeInfo,
      LockCache<String> lockCache) {
      return new FileSnapshotStore(runtimeInfo, lockCache);
    }
  }

  /*************** PipelineManager ***************/

  @Module(
    injects = {
      StandaloneAndClusterPipelineManager.class,
      StandaloneRunner.class
    },
    library = true,
    includes = {
      TestPipelineStoreModuleNew.class,
      TestExecutorModule.class,
      TestSnapshotStoreModule.class,
      TestAclStoreModule.class,
      TestLineageModule.class
    }
  )
  public static class TestPipelineManagerModule {

    public TestPipelineManagerModule() {
    }

    @Provides @Singleton
    public PreviewerProvider providePreviewerProvider() {
      return new PreviewerProvider() {
        @Override
        public Previewer createPreviewer(String user, String name, String rev, PreviewerListener listener,
                                         ObjectGraph objectGraph) {
          Previewer mock = Mockito.mock(Previewer.class);
          Mockito.when(mock.getId()).thenReturn(UUID.randomUUID().toString());
          Mockito.when(mock.getName()).thenReturn(name);
          Mockito.when(mock.getRev()).thenReturn(rev);
          return mock;
        }
      };
    }

    @Provides @Singleton
    public RunnerProvider provideRunnerProvider() {
      return (name, rev, objectGraph, executionMode) -> {
        ObjectGraph plus = objectGraph.plus(new TestPipelineProviderModule(name, rev));
        TestRunnerModule testRunnerModule = new TestRunnerModule(name, rev, plus);
        return testRunnerModule.provideRunner(
            new SafeScheduledExecutorService(1, "runnerExecutor"),
            new SafeScheduledExecutorService(1, "runnerStopExecutor")
        );
      };
    }

  }

}
