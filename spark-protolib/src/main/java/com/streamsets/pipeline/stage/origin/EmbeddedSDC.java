/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.BlackListURLClassLoader;
import com.streamsets.pipeline.BootstrapMain;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.LogConfigurator;
import com.streamsets.pipeline.main.Main;
import com.streamsets.pipeline.main.PipelineTask;
import com.streamsets.pipeline.main.PipelineTaskModule;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.PipelineConfigurationJson;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class EmbeddedSDC {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSDC.class);
  private static final List<EmbeddedSDC> instances = new CopyOnWriteArrayList<>();
  private final AtomicLong numRecordsProduced = new AtomicLong(0);
  private ProductionPipelineManagerTask productionPipelineManagerTask;
  private ObjectGraph dagger;
  private volatile SparkStreamingSource sparkStreamingSource;
  private Thread waitingThread;
  private EmbeddedSDCConf sdcConf;

  public EmbeddedSDC(EmbeddedSDCConf sdcConf) {
    this.sdcConf = sdcConf;
  }

  public void put(List<String> batch) throws InterruptedException {
    sparkStreamingSource.put(batch);
    numRecordsProduced.addAndGet(batch.size());
  }
  public void init() throws Exception {
    this.instances.add(this);
    String classLoaderName = "embedded-sdc";
    BlackListURLClassLoader blackListURLClassLoader = new BlackListURLClassLoader(classLoaderName, sdcConf.getClassPath(),
      ClassLoader.getSystemClassLoader(), BootstrapMain.PACKAGES_BLACKLIST_FOR_STAGE_LIBRARIES);
    RuntimeModule.setStageLibraryClassLoaders(Arrays.asList(blackListURLClassLoader));
    dagger = ObjectGraph.create(PipelineTaskModule.class);
    final Task task = dagger.get(TaskWrapper.class);
    PipelineTask pipelineTask = (PipelineTask) ((TaskWrapper)task).getTask();
    productionPipelineManagerTask = pipelineTask.getProductionPipelineManagerTask();
    dagger.get(LogConfigurator.class).configure();
    final Logger log = LoggerFactory.getLogger(Main.class);
    log.info("-----------------------------------------------------------------");
    dagger.get(BuildInfo.class).log(log);
    log.info("-----------------------------------------------------------------");
    dagger.get(RuntimeInfo.class).log(log);
    log.info("-----------------------------------------------------------------");
    if (System.getSecurityManager() != null) {
      log.info("  Security Manager : ENABLED, policy file: {}", System.getProperty("java.security.policy"));
    } else {
      log.warn("  Security Manager : DISABLED");
    }
    log.info("-----------------------------------------------------------------");
    log.info("Starting ...");

    task.init();
    final Logger finalLog = log;
    final Thread shutdownHookThread = new Thread("Main.shutdownHook") {
      @Override
      public void run() {
        finalLog.debug("Stopping, reason: SIGTERM (kill)");
        task.stop();
      }
    };
    Runtime.getRuntime().addShutdownHook(shutdownHookThread);
    dagger.get(RuntimeInfo.class).setShutdownHandler(new Runnable() {
      @Override
      public void run() {
        finalLog.debug("Stopping, reason: requested");
        task.stop();
      }
    });
    task.run();
    StageLibraryTask stageLibrary = pipelineTask.getStageLibraryTask();
    EmbeddedPipeline embeddedPipeline = sdcConf.getPipeline();
    PipelineConfigurationJson pipelineConfigBean = embeddedPipeline.getDefinitionJson();
    PipelineStoreTask store = pipelineTask.getPipelineStoreTask();
    // we might want to add an import API as now to import have to create one then update it
    com.streamsets.pipeline.config.PipelineConfiguration tmpPipelineConfig = store.
      create(embeddedPipeline.getName(), embeddedPipeline.getDescription(), embeddedPipeline.getUser());
    com.streamsets.pipeline.config.PipelineConfiguration realPipelineConfig = BeanHelper.unwrapPipelineConfiguration(
      pipelineConfigBean);
    realPipelineConfig.setUuid(tmpPipelineConfig.getUuid());
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary,
      embeddedPipeline.getName(), realPipelineConfig);
    validator.validate();
    realPipelineConfig.setValidation(validator);
    realPipelineConfig = store.save(embeddedPipeline.getName(), embeddedPipeline.getUser(), embeddedPipeline.getTag(),
      embeddedPipeline.getDescription(), realPipelineConfig);
    productionPipelineManagerTask.startPipeline(embeddedPipeline.getName(), "1");
    Pipeline pipeline = productionPipelineManagerTask.getProductionPipeline().getPipeline();
    productionPipelineManagerTask.getMetrics().getCounters();
    sparkStreamingSource = (SparkStreamingSource) pipeline.getSource();
    // this thread waits until the pipeline is shutdown
    waitingThread = new Thread() {
      public void run() {
        try {
          task.waitWhileRunning();
          try {
            Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
          } catch (IllegalStateException ignored) {
          }
          log.debug("Stopping, reason: programmatic stop()");
        } catch(Throwable throwable) {
          String msg = "Error running pipeline: " + throwable;
          log.error(msg, throwable);
        }
      }
    };
    waitingThread.setName("");
    waitingThread.setDaemon(true);
    waitingThread.start();;
  }

  public static long getRecordsProducedJVMWide() {
    long result = 0;
    for (EmbeddedSDC sdc : instances) {
      result += sdc.numRecordsProduced.get();
    }
    return result;
  }
}
