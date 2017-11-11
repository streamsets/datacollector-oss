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
package com.streamsets.datacollector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.http.ServerNotYetRunningException;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.LogConfigurator;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.PipelineTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.ShutdownHandler;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.impl.DataCollector;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a copy of EmbeddedDataCollector which implements start, stop and create pipeline APIs used by the mini
 * integration tests.
 *
 * This class needs to be moved to the test package or a different module once the class loading is figured out.
 */
public class MiniITDataCollector implements DataCollector {
  private static final Logger LOG = LoggerFactory.getLogger(MiniITDataCollector.class);
  private String pipelineName;
  private String pipelineRev;
  private Manager pipelineManager;
  private ObjectGraph dagger;
  private Thread waitingThread;
  private PipelineConfiguration realPipelineConfig;
  private PipelineTask pipelineTask;
  private Task task;
  private Runner runner;

  private void createAndSave(String pipelineName) throws PipelineException {
    String user = realPipelineConfig.getInfo().getCreator();
    String tag = realPipelineConfig.getInfo().getLastRev();
    String desc = realPipelineConfig.getDescription();
    StageLibraryTask stageLibrary = pipelineTask.getStageLibraryTask();
    PipelineStoreTask store = pipelineTask.getPipelineStoreTask();
    PipelineConfiguration tmpPipelineConfig =
      store.create(user, pipelineName, pipelineName, desc, false, false);
    // we might want to add an import API as now to import have to create one then update it
    realPipelineConfig.setUuid(tmpPipelineConfig.getUuid());
    PipelineConfigurationValidator validator =
      new PipelineConfigurationValidator(stageLibrary, pipelineName, realPipelineConfig);
    realPipelineConfig = validator.validate();
    realPipelineConfig = store.save(user, pipelineName, tag, desc, realPipelineConfig);
  }

  @Override
  public void startPipeline(String pipelineJson) throws Exception {
    Utils.checkNotNull(pipelineJson, "Pipeline Json string");
    ObjectMapper json = ObjectMapperFactory.getOneLine();
    PipelineConfigurationJson pipelineConfigBean = json.readValue(pipelineJson, PipelineConfigurationJson.class);
    realPipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipelineConfigBean);
    if (task == null) {
      throw new IllegalStateException("Data collector has not been started");
    }
    pipelineTask = (PipelineTask) ((TaskWrapper)task).getTask();
    this.pipelineName = Utils.checkNotNull(realPipelineConfig.getInfo(), "Pipeline Info").getPipelineId();
    this.pipelineRev = Utils.checkNotNull(realPipelineConfig.getInfo(), "Pipeline Info").getLastRev();
    createAndSave(pipelineName);
    runner = pipelineManager.getRunner(pipelineName, pipelineRev);
    runner.start(realPipelineConfig.getInfo().getCreator());
  }

  @Override
  public void createPipeline(String pipelineJson) throws Exception {
    Utils.checkNotNull(pipelineJson, "Pipeline Json string");
    ObjectMapper json = ObjectMapperFactory.getOneLine();
    PipelineConfigurationJson pipelineConfigBean = json.readValue(pipelineJson, PipelineConfigurationJson.class);
    realPipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipelineConfigBean);
    if (task == null) {
      throw new IllegalStateException("Data collector has not been started");
    }
    pipelineTask = (PipelineTask) ((TaskWrapper)task).getTask();
    this.pipelineName = Utils.checkNotNull(realPipelineConfig.getInfo(), "Pipeline Info").getPipelineId();
    this.pipelineRev = Utils.checkNotNull(realPipelineConfig.getInfo(), "Pipeline Info").getLastRev();
    createAndSave(pipelineName);
  }

  @Override
  public void startPipeline() throws Exception {
    Utils.checkNotNull(pipelineName, "No pipeline to run");
    runner = pipelineManager.getRunner(pipelineName, pipelineRev);
    runner.start(realPipelineConfig.getInfo().getCreator());
  }

  @Override
  public void stopPipeline() throws Exception {
    Utils.checkNotNull(pipelineName, "No pipeline to stop");
    runner.stop(realPipelineConfig.getInfo().getCreator());
  }

  @Override
  public void init() {
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    LOG.info("Entering Embedded SDC with ClassLoader: " + classLoader);
    LOG.info("Java classpath is " + System.getProperty("java.class.path"));
    dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);
    task = dagger.get(TaskWrapper.class);
    pipelineTask = (PipelineTask) ((TaskWrapper)task).getTask();
    pipelineManager = pipelineTask.getManager();
    dagger.get(LogConfigurator.class).configure();
    LOG.info("-----------------------------------------------------------------");
    dagger.get(BuildInfo.class).log(LOG);
    LOG.info("-----------------------------------------------------------------");
    dagger.get(RuntimeInfo.class).log(LOG);
    LOG.info("-----------------------------------------------------------------");
    if (System.getSecurityManager() != null) {
      LOG.info("  Security Manager : ENABLED, policy file: {}", System.getProperty("java.security.policy"));
    } else {
      LOG.warn("  Security Manager : DISABLED");
    }
    LOG.info("-----------------------------------------------------------------");
    LOG.info("Starting ...");

    task.init();
    final Thread shutdownHookThread = new Thread("Main.shutdownHook") {
      @Override
      public void run() {
        LOG.debug("Stopping, reason: SIGTERM (kill)");
        task.stop();
      }
    };
    shutdownHookThread.setContextClassLoader(classLoader);
    Runtime.getRuntime().addShutdownHook(shutdownHookThread);
    dagger.get(RuntimeInfo.class).setShutdownHandler(new ShutdownHandler(LOG, task, new ShutdownHandler.ShutdownStatus()));
    task.run();

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
          LOG.debug("Stopping, reason: programmatic stop()");
        } catch(Throwable throwable) {
          String msg = "Error running pipeline: " + throwable;
          LOG.error(msg, throwable);
        }
      }
    };
    waitingThread.setContextClassLoader(classLoader);
    waitingThread.setName("Pipeline-" + pipelineName);
    waitingThread.setDaemon(true);
    waitingThread.start();
  }

  @Override
  public URI getServerURI() {
    URI serverURI;
    try {
      serverURI =  pipelineTask.getWebServerTask().getServerURI();
    } catch (ServerNotYetRunningException ex) {
      throw new RuntimeException("Cannot retrieve URI of server" + ex.toString(), ex);
    }
    return serverURI;
  }

  @Override
  public void destroy() {
    if (task != null) {
      task.stop();
    }
  }

  @Override
  public List<URI> getWorkerList() throws URISyntaxException {
    List<URI> sdcURLList = new ArrayList<>();
    for (CallbackInfo callBackInfo : runner.getSlaveCallbackList(CallbackObjectType.METRICS) ) {
      sdcURLList.add(new URI(callBackInfo.getSdcURL()));
    }
    return sdcURLList;
  }

  @Override
  public String storeRules(String name, String tag, String ruleDefinitionsJsonString) throws Exception {
    Utils.checkNotNull(ruleDefinitionsJsonString, "Rule Definition Json string");
    ObjectMapper json = ObjectMapperFactory.getOneLine();
    RuleDefinitionsJson ruleDefinitionsJson = json.readValue(ruleDefinitionsJsonString, RuleDefinitionsJson.class);
    RuleDefinitions ruleDefinitions = BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson);
    RuleDefinitions ruleDefinitions1 = pipelineTask.getPipelineStoreTask().storeRules(name, tag, ruleDefinitions, false);
    return ObjectMapperFactory.get().writeValueAsString(BeanHelper.wrapRuleDefinitions(ruleDefinitions1));
  }

  @Override
  public List<Issue> validatePipeline(String name, String pipelineJson) throws IOException {
    final ObjectMapper json = ObjectMapperFactory.get();
    final PipelineConfigurationJson pipeline = json.readValue(pipelineJson,
        PipelineConfigurationJson.class);
    PipelineConfiguration pipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipeline);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(pipelineTask.getStageLibraryTask(), name, pipelineConfig);
    validator.validate();
    return validator.getIssues().getIssues();
  }
}
