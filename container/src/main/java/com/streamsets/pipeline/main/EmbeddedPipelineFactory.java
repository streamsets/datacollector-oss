package com.streamsets.pipeline.main;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.PipelineConfigurationJson;
import com.streamsets.pipeline.runner.BatchListener;
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

import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class EmbeddedPipelineFactory {
  private static final String PREFIX = "streamsets.cluster.pipeline.";
  public static final String PIPELINE_NAME = PREFIX + "name";
  public static final String PIPELINE_DESCRIPTION = PREFIX + "description";
  public static final String PIPELINE_USER = PREFIX + "user";
  public static final String PIPELINE_TAG = PREFIX + "tag";

  public static Source createPipeline(Properties properties, String pipelineJson,
                                      final Runnable postBatchRunnable) throws Exception {
    String pipelineName = Utils.checkNotNull(properties.getProperty(PIPELINE_NAME), PIPELINE_NAME);
    String pipelineDesc = Utils.checkNotNull(properties.getProperty(PIPELINE_DESCRIPTION), PIPELINE_DESCRIPTION);
    String pipelineUser = Utils.checkNotNull(properties.getProperty(PIPELINE_USER), PIPELINE_USER);
    String pipelineTag = Utils.checkNotNull(properties.getProperty(PIPELINE_TAG), PIPELINE_TAG);
    ObjectMapper json = ObjectMapperFactory.get();
    // TODO fix this terrible hack
    String rawPipelineConfig = json.writeValueAsString(json.readValue(pipelineJson, Map.class).get("pipelineConfig"));
    PipelineConfigurationJson pipelineConfigBean = json.readValue(rawPipelineConfig, PipelineConfigurationJson.class);
    EmbeddedPipeline embeddedPipeline = new EmbeddedPipeline(pipelineName, pipelineUser, pipelineTag,
      pipelineDesc, pipelineConfigBean);
    Pipeline realPipeline = embeddedPipeline.create();
    realPipeline.getRunner().registerListener(new BatchListener() {
      @Override
      public void preBatch() {
        // nothing
      }

      @Override
      public void postBatch() {
        postBatchRunnable.run();
      }
    });
    return realPipeline.getSource();

  }


  private static class EmbeddedPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedPipeline.class);
    private String piplineName;
    private String pipelineUser;
    private String pipelineTag;
    private String pipelineDescription;
    private PipelineManager pipelineManager;
    private ObjectGraph dagger;
    private Thread waitingThread;
    private PipelineConfigurationJson pipelineConfigBean;

    public EmbeddedPipeline(String pipelineName, String pipelineUser, String pipelineTag, String pipelineDescription,
                            PipelineConfigurationJson pipelineConfigBean) throws Exception {
      this.piplineName = pipelineName + "-" + (new Random()).nextInt(Integer.MAX_VALUE);
      this.pipelineUser = pipelineUser;
      this.pipelineTag = pipelineTag;
      this.pipelineDescription = pipelineDescription;
      this.pipelineConfigBean = pipelineConfigBean;
    }

    private void createAndSave(PipelineTask pipelineTask, String pipelineName) throws PipelineStoreException {
      StageLibraryTask stageLibrary = pipelineTask.getStageLibraryTask();
      PipelineStoreTask store = pipelineTask.getPipelineStoreTask();
      PipelineConfiguration tmpPipelineConfig =
        store.create(pipelineName, pipelineDescription, pipelineUser);
      // we might want to add an import API as now to import have to create one then update it
      PipelineConfiguration realPipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipelineConfigBean);
      realPipelineConfig.setUuid(tmpPipelineConfig.getUuid());
      PipelineConfigurationValidator validator =
        new PipelineConfigurationValidator(stageLibrary, pipelineName, realPipelineConfig);
      validator.validate();
      realPipelineConfig.setValidation(validator);
      realPipelineConfig =
        store.save(pipelineName, pipelineUser, pipelineTag, pipelineDescription, realPipelineConfig);
    }

    private Pipeline create() throws Exception {
      final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      LOG.info("Entering Embedded SDC with ClassLoader: " + classLoader);
      dagger = ObjectGraph.create(PipelineTaskModule.class);
      final Task task = dagger.get(TaskWrapper.class);
      PipelineTask pipelineTask = (PipelineTask) ((TaskWrapper)task).getTask();
      pipelineManager = pipelineTask.getProductionPipelineManagerTask();
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
      dagger.get(RuntimeInfo.class).setShutdownHandler(new Runnable() {
        @Override
        public void run() {
          LOG.debug("Stopping, reason: requested");
          task.stop();
        }
      });
      task.run();

      createAndSave(pipelineTask, piplineName);
      pipelineManager.startPipeline(piplineName, "1");
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
      waitingThread.setName("Pipeline-" + piplineName);
      waitingThread.setDaemon(true);
      waitingThread.start();
      return pipelineManager.getProductionPipeline().getPipeline();
    }
  }
}
