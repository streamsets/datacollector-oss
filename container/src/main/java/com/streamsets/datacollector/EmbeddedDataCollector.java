/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.datacollector;

import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineInfo;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.http.ServerNotYetRunningException;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.LogConfigurator;
import com.streamsets.datacollector.main.MainSlavePipelineManagerModule;
import com.streamsets.datacollector.main.PipelineTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.impl.DataCollector;

import dagger.ObjectGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmbeddedDataCollector implements DataCollector {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedDataCollector.class);
  private String pipelineName;
  private Manager pipelineManager;
  private ObjectGraph dagger;
  private Thread waitingThread;
  private Task task;
  private RuntimeInfo runtimeInfo;
  private Runner runner;
  private PipelineTask pipelineTask;


  @Override
  public void startPipeline() throws Exception {
    File sdcProperties = new File(runtimeInfo.getDataDir(), "sdc.properties");
    Utils.checkState(sdcProperties.exists(), Utils.format("sdc property file doesn't exist at '{}'",
      sdcProperties.getAbsolutePath()));
    Properties properties = new Properties();
    properties.load(new FileInputStream(sdcProperties));
    String pipelineName = Utils.checkNotNull(properties.getProperty("cluster.pipeline.name"), "Pipeline name");
    String pipelineUser = Utils.checkNotNull(properties.getProperty("cluster.pipeline.user"), "Pipeline user");
    String pipelineRev = Utils.checkNotNull(properties.getProperty("cluster.pipeline.rev"), "Pipeline revision");
    runner = pipelineManager.getRunner(pipelineUser, pipelineName, pipelineRev);
    runner.start();
  }

  @Override
  public void createPipeline(String pipelineJson) throws Exception {
    throw new UnsupportedOperationException("This method is not supported. Use \"startPipeline\" method");

  }

  @Override
  public void stopPipeline() throws Exception {
    throw new UnsupportedOperationException("This method is not supported. Use \"startPipeline\" method");
  }

  @Override
  public void init() {
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    LOG.info("Entering Embedded SDC with ClassLoader: " + classLoader);
    LOG.info("Java classpath is " + System.getProperty("java.class.path"));
    dagger = ObjectGraph.create(MainSlavePipelineManagerModule.class);
    task = dagger.get(TaskWrapper.class);
    pipelineTask = (PipelineTask) ((TaskWrapper)task).getTask();
    pipelineName = pipelineTask.getName();
    pipelineManager = pipelineTask.getManager();
    runtimeInfo = dagger.get(RuntimeInfo.class);
    dagger.get(LogConfigurator.class).configure();
    LOG.info("-----------------------------------------------------------------");
    dagger.get(BuildInfo.class).log(LOG);
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
    dagger.get(RuntimeInfo.class).setShutdownHandler(new Runnable() {
      @Override
      public void run() {
        LOG.debug("Stopping, reason: requested");
        task.stop();
      }
    });
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
      throw new RuntimeException("Cannot retrieve URI of server" + ex.getMessage(), ex);
    }
    return serverURI;
  }

  @Override
  public void destroy() {
    task.stop();
  }

  public Pipeline getPipeline() {
    return ((PipelineInfo)runner).getPipeline();
  }

  @Override
  public List<URI> getWorkerList() throws URISyntaxException {
    List<URI> sdcURLList = new ArrayList<>();
    for (CallbackInfo callBackInfo : runner.getSlaveCallbackList()) {
      sdcURLList.add(new URI(callBackInfo.getSdcURL()));
    }
    return sdcURLList;
  }

  @Override
  public void startPipeline(String pipelineJson) throws Exception {
    throw new UnsupportedOperationException("This method is not supported. Use \"startPipeline()\" method");
  }

  @Override
  public String storeRules(String name, String tag, String ruleDefinitionsJsonString) throws Exception {
    throw new UnsupportedOperationException("This method is not supported.");
  }

}
