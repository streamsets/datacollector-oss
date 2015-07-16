/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.CreateByRef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.creation.PipelineBean;
import com.streamsets.pipeline.creation.StageBean;
import com.streamsets.pipeline.validation.Issue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class StageRuntime {
  private final PipelineBean pipelineBean;
  private final StageDefinition def;
  private final StageConfiguration conf;
  private final StageBean stageBean;
  private final Stage.Info info;
  private StageContext context;

  public StageRuntime(PipelineBean pipelineBean, final StageBean stageBean) {
    this.pipelineBean = pipelineBean;
    this.def = stageBean.getDefinition();
    this.stageBean = stageBean;
    this.conf = stageBean.getConfiguration();
    info = new Stage.Info() {
      @Override
      public String getName() {
        return def.getName();
      }

      @Override
      public String getVersion() {
        return def.getVersion();
      }

      @Override
      public String getInstanceName() {
        return conf.getInstanceName();
      }

      @Override
      public String toString() {
        return Utils.format("Info[instance='{}' name='{}' version='{}']", getInstanceName(), getName(), getVersion());
      }
    };

  }

  public Map<String, Object> getConstants() {
    return pipelineBean.getConfig().constants;
  }

  public StageDefinition getDefinition() {
    return def;
  }

  public StageConfiguration getConfiguration() {
    return conf;
  }

  public List<String> getRequiredFields() {
    return stageBean.getSystemConfigs().stageRequiredFields;
  }

  public List<String> getPreconditions() {
    return stageBean.getSystemConfigs().stageRecordPreconditions;
  }

  public OnRecordError getOnRecordError() {
    return stageBean.getSystemConfigs().stageOnRecordError;
  }

  public Stage getStage() {
    return stageBean.getStage();
  }

  public void setContext(StageContext context) {
    this.context = context;
  }

  public void setErrorSink(ErrorSink errorSink) {
    context.setErrorSink(errorSink);
  }

  @SuppressWarnings("unchecked")
  public <T extends Stage.Context> T getContext() {
    return (T) context;
  }

  @SuppressWarnings("unchecked")
  public List<Issue> init() {
    Preconditions.checkState(context != null, "context has not been set");
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
      List<Issue> issues = getStage().init(info, context);
      if (issues == null) {
        issues = Collections.emptyList();
      }
      return issues;
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  String execute(Callable<String> callable) throws Exception {
    // if the stage is annotated as recordsByRef it means it does not reuse the records/fields it creates, thus
    // we have to call it within a create-by-ref context so Field.create does not clone Fields and BatchMakerImpl
    // does not clone output records.
    return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : callable.call();
  }

  public String execute(final String previousOffset, final int batchSize, final Batch batch,
      final BatchMaker batchMaker, ErrorSink errorSink) throws StageException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      setErrorSink(errorSink);
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());

      Callable<String> callable = new Callable<String>() {
        @Override
        public String call() throws Exception {
          String newOffset = null;
          switch (getDefinition().getType()) {
            case SOURCE: {
              newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker);
              break;
            }
            case PROCESSOR: {
              ((Processor) getStage()).process(batch, batchMaker);
              break;

            }
            case TARGET: {
              ((Target) getStage()).write(batch);
              break;
            }
          }
          return newOffset;
        }
      };

      try {
        return execute(callable);
      } catch (Exception ex) {
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else if (ex instanceof RuntimeException) {
          throw (RuntimeException) ex;
        } else {
          throw new RuntimeException(ex);
        }
      }

    } finally {
      setErrorSink(null);
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  public void destroy() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
      getStage().destroy();
    } finally {
      //we release the stage classloader back to the library  ro reuse (as some stages my have private classloaders)
      stageBean.releaseClassLoader();
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  public Stage.Info getInfo() {
    return info;
  }

}
