/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;


import com.codahale.metrics.Counter;
import com.streamsets.pipeline.BlackListURLClassLoader;
import com.streamsets.pipeline.BootstrapMain;
import com.streamsets.pipeline.config.PipelineConfiguration;
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
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;

import dagger.ObjectGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;
import java.util.SortedMap;


public class EmbeddedSDC {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSDC.class);
  private ProductionPipelineManagerTask productionPipelineManagerTask;
  private ObjectGraph dagger;
  private Thread waitingThread;
  private EmbeddedSDCConf sdcConf;

  public EmbeddedSDC(EmbeddedSDCConf sdcConf) {
    this.sdcConf = sdcConf;
  }

  private void createAndSave(PipelineTask pipelineTask, String pipelineName) throws PipelineStoreException {
    StageLibraryTask stageLibrary = pipelineTask.getStageLibraryTask();
    PipelineStoreTask store = pipelineTask.getPipelineStoreTask();
    EmbeddedPipeline embeddedPipeline = sdcConf.getPipeline();
    PipelineConfigurationJson pipelineConfigBean = embeddedPipeline.getDefinitionJson();
    PipelineConfiguration tmpPipelineConfig =
      store.create(pipelineName, embeddedPipeline.getDescription(), embeddedPipeline.getUser());
    // we might want to add an import API as now to import have to create one then update it
    PipelineConfiguration realPipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipelineConfigBean);
    realPipelineConfig.setUuid(tmpPipelineConfig.getUuid());

    PipelineConfigurationValidator validator =
      new PipelineConfigurationValidator(stageLibrary, pipelineName, realPipelineConfig);
    validator.validate();
    realPipelineConfig.setValidation(validator);
    realPipelineConfig =
      store.save(pipelineName, embeddedPipeline.getUser(), embeddedPipeline.getTag(),
        embeddedPipeline.getDescription(), realPipelineConfig);
  }

  public Pipeline init() throws Exception {
    EmbeddedPipeline embeddedPipeline = sdcConf.getPipeline();
    String pipeLineName = embeddedPipeline.getName() + new Random().nextInt(Integer.MAX_VALUE);
    LOG.info("Entering Embedded SDC");
    String classLoaderName = "embedded-sdc";
    BlackListURLClassLoader blackListURLClassLoader = new BlackListURLClassLoader(classLoaderName, sdcConf.getClassPath(),
      ClassLoader.getSystemClassLoader(), BootstrapMain.PACKAGES_BLACKLIST_FOR_STAGE_LIBRARIES);
    RuntimeModule.setStageLibraryClassLoaders(Arrays.asList(blackListURLClassLoader));
    dagger = ObjectGraph.create(PipelineTaskModule.class);
    final Task task = dagger.get(TaskWrapper.class);
    PipelineTask pipelineTask = (PipelineTask) ((TaskWrapper)task).getTask();
    productionPipelineManagerTask = pipelineTask.getProductionPipelineManagerTask();
    dagger.get(LogConfigurator.class).configure();
    final Logger log = LoggerFactory.getLogger(EmbeddedSDC.class);
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

    createAndSave(pipelineTask, pipeLineName);
    productionPipelineManagerTask.startPipeline(pipeLineName, "1");
    // this thread waits until the pipeline is shutdown
    waitingThread = new Thread() {
      @Override
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
    waitingThread.start();
    return getPipeline();
  }

  Pipeline getPipeline() {
    return productionPipelineManagerTask.getProductionPipeline().getPipeline();
  }

  SortedMap<String, Counter> getCounters() {
    return productionPipelineManagerTask.getMetrics().getCounters();
  }

}
