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

  public static Source createPipeline(Properties properties, String pipelineJson,
                                      final Runnable postBatchRunnable) throws Exception {
    ObjectMapper json = ObjectMapperFactory.getOneLine();
    PipelineConfigurationJson pipelineConfigBean = json.readValue(pipelineJson, PipelineConfigurationJson.class);
    EmbeddedPipeline embeddedPipeline = new EmbeddedPipeline(pipelineConfigBean);
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
    private PipelineManager pipelineManager;
    private ObjectGraph dagger;
    private Thread waitingThread;
    private PipelineConfiguration realPipelineConfig;

    public EmbeddedPipeline(PipelineConfigurationJson pipelineConfigBean) throws Exception {
      realPipelineConfig = Utils.checkNotNull(BeanHelper.unwrapPipelineConfiguration(Utils.
          checkNotNull(pipelineConfigBean, "Pipeline Config Bean")), "Pipeline Config");
      this.piplineName = Utils.checkNotNull(realPipelineConfig.getInfo(), "Pipeline Info")
        .getName() + "-" + (new Random()).nextInt(Integer.MAX_VALUE);
    }

    private void createAndSave(PipelineTask pipelineTask, String pipelineName) throws PipelineStoreException {
      String user = realPipelineConfig.getInfo().getCreator();
      String tag = realPipelineConfig.getInfo().getLastRev();
      String desc = realPipelineConfig.getDescription();
      StageLibraryTask stageLibrary = pipelineTask.getStageLibraryTask();
      PipelineStoreTask store = pipelineTask.getPipelineStoreTask();
      PipelineConfiguration tmpPipelineConfig =
        store.create(pipelineName, desc, user);
      // we might want to add an import API as now to import have to create one then update it
      realPipelineConfig.setUuid(tmpPipelineConfig.getUuid());
      PipelineConfigurationValidator validator =
        new PipelineConfigurationValidator(stageLibrary, pipelineName, realPipelineConfig);
      validator.validate();
      realPipelineConfig.setValidation(validator);
      realPipelineConfig =
        store.save(pipelineName, user, tag, desc, realPipelineConfig);
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
