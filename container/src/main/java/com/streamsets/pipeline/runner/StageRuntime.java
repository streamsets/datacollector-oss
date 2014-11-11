/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.runner;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.stagelibrary.StageLibrary;

import java.lang.reflect.Field;

public class StageRuntime {
  private final StageDefinition def;
  private final StageConfiguration conf;
  private final Stage stage;
  private final Stage.Info info;
  private Stage.Context context;

  private StageRuntime(final StageDefinition def, final StageConfiguration conf, Stage stage) {
    this.def = def;
    this.conf = conf;
    this.stage = stage;
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
    };
  }

  public StageDefinition getDefinition() {
    return def;
  }

  public StageConfiguration getConfiguration() {
    return conf;
  }

  public Stage getStage() {
    return stage;
  }

  public void setContext(Stage.Context context) {
    this.context = context;
  }

  @SuppressWarnings("unchecked")
  public <T extends Stage.Context> T getContext() {
    return (T) context;
  }

  @SuppressWarnings("unchecked")
  public void init() throws StageException {
    Preconditions.checkState(context != null, "context has not been set");
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getClassLoader());
      stage.init(info, context);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  public String execute(String previousOffset, int batchSize, Batch batch, BatchMaker batchMaker)
      throws StageException {
    String newOffset = null;
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getClassLoader());
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
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
    return newOffset;
  }

  public void destroy() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getClassLoader());
      stage.destroy();
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  public Stage.Info getInfo() {
    return info;
  }

  public static class Builder {
    private final StageLibrary stageLib;
    private final String name;
    private final PipelineConfiguration pipelineConf;

    public Builder(StageLibrary stageLib, String name, PipelineConfiguration pipelineConf) {
      this.stageLib = stageLib;
      this.name = name;
      this.pipelineConf = pipelineConf;
    }

    public StageRuntime[] build() throws PipelineRuntimeException {
      PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLib, name, pipelineConf);
      if (!validator.validate()) {
        throw new PipelineRuntimeException(PipelineRuntimeException.ERROR.PIPELINE_CONFIGURATION, validator.getIssues());
      }
      try {
        StageRuntime[] runtimes = new StageRuntime[pipelineConf.getStages().size()];
        for (int i = 0; i < pipelineConf.getStages().size(); i++) {
          StageConfiguration conf = pipelineConf.getStages().get(i);
          StageDefinition def = stageLib.getStage(conf.getLibrary(), conf.getStageName(), conf.getStageVersion());
          Class klass = def.getClassLoader().loadClass(def.getClassName());
          Stage stage = (Stage) klass.newInstance();
          configureStage(def, conf, klass, stage);
          runtimes[i] = new StageRuntime(def, conf, stage);
        }
        return runtimes;
      } catch (PipelineRuntimeException ex) {
        throw ex;
        } catch (Exception ex) {
        throw new PipelineRuntimeException(PipelineRuntimeException.ERROR.PIPELINE_BUILD, ex.getMessage(), ex);
      }
    }

    private void configureStage(StageDefinition stageDef, StageConfiguration stageConf, Class klass, Stage stage)
        throws PipelineRuntimeException {
      for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
        ConfigConfiguration confConf = stageConf.getConfig(confDef.getName());
        if (confConf == null) {
          throw new PipelineRuntimeException(PipelineRuntimeException.ERROR.STAGE_MISSING_CONFIG,
                                             stageDef.getClassName(), stageConf.getInstanceName(), confDef.getName());
        }
        Object value = confConf.getValue();
        String instanceVar = confDef.getFieldName();
        try {
          Field var = klass.getField(instanceVar);
          var.set(stage, value);
        } catch (Exception ex) {
          throw new PipelineRuntimeException(PipelineRuntimeException.ERROR.STAGE_CONFIG_INJECTION,
                                           stageDef.getClassName(), stageConf.getInstanceName(), instanceVar, value,
                                           ex.getMessage(), ex);
        }
      }
    }

  }

}
