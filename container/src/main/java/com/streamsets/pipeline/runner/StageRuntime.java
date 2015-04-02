/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.validation.StageIssue;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StageRuntime {
  private final StageDefinition def;
  private final StageConfiguration conf;
  private final Stage stage;
  private final Stage.Info info;
  private final List<String> requiredFields;
  private final OnRecordError onRecordError;
  private StageContext context;
  private final Map<String, Object> constants;

  private StageRuntime(final StageDefinition def, final StageConfiguration conf, List<String> requiredFields,
      OnRecordError onRecordError, Stage stage, Map<String, Object> constants) {
    this.def = def;
    this.conf = conf;
    this.requiredFields = requiredFields;
    this.onRecordError = onRecordError;
    this.stage = stage;
    this.constants = constants;
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

  public StageDefinition getDefinition() {
    return def;
  }

  public StageConfiguration getConfiguration() {
    return conf;
  }

  public List<String> getRequiredFields() {
    return requiredFields;
  }

  public OnRecordError getOnRecordError() {
    return onRecordError;
  }

  public Stage getStage() {
    return stage;
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
  public List<StageIssue> validateConfigs() throws StageException {
    Preconditions.checkState(context != null, "context has not been set");
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
      List<StageIssue> issues = stage.validateConfigs(info, context);
      if (issues == null) {
        issues = Collections.emptyList();
      }
      return issues;
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  @SuppressWarnings("unchecked")
  public void init() throws StageException {
    Preconditions.checkState(context != null, "context has not been set");
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
      stage.init(info, context);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  public String execute(String previousOffset, int batchSize, Batch batch, BatchMaker batchMaker,
      ErrorSink errorSink)
      throws StageException {
    String newOffset = null;
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      setErrorSink(errorSink);
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
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
      setErrorSink(null);
      Thread.currentThread().setContextClassLoader(cl);
    }
    return newOffset;
  }

  public void destroy() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
      stage.destroy();
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  public Stage.Info getInfo() {
    return info;
  }

  public static class Builder {
    private final StageLibraryTask stageLib;
    private final String name;
    private final PipelineConfiguration pipelineConf;
    private List<String> requiredFields;
    private OnRecordError onRecordError;

    public Builder(StageLibraryTask stageLib, String name, PipelineConfiguration pipelineConf) {
      this.stageLib = stageLib;
      this.name = name;
      this.pipelineConf = pipelineConf;
      onRecordError = OnRecordError.STOP_PIPELINE;
    }

    public StageRuntime[] build() throws PipelineRuntimeException {
      PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLib, name, pipelineConf);
      if (!validator.validate()) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0150, validator.getIssues());
      }
      try {
        StageRuntime[] runtimes = new StageRuntime[pipelineConf.getStages().size()];
        for (int i = 0; i < pipelineConf.getStages().size(); i++) {
          runtimes[i] =  buildStage(pipelineConf.getStages().get(i), pipelineConf);
        }
        return runtimes;
      } catch (PipelineRuntimeException ex) {
        throw ex;
        } catch (Exception ex) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0151, ex.getMessage(), ex);
      }
    }

    public StageRuntime buildErrorStage(PipelineConfiguration pipelineConf) throws PipelineRuntimeException {
      return buildStage(pipelineConf.getErrorStage(), pipelineConf);
    }

    private StageRuntime buildStage(StageConfiguration conf, PipelineConfiguration pipelineConf)
      throws PipelineRuntimeException {
      try {
        StageDefinition def = stageLib.getStage(conf.getLibrary(), conf.getStageName(), conf.getStageVersion());
        Class klass = def.getStageClassLoader().loadClass(def.getClassName());
        Stage stage = (Stage) klass.newInstance();
        configureStage(def, conf, klass, stage);
        return new StageRuntime(def, conf, requiredFields, onRecordError, stage, getConstants(pipelineConf));
      } catch (Exception ex) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0151, ex.getMessage(), ex);
      }
    }

    private Map<String, Object> getConstants(PipelineConfiguration pipelineConf) {
      Map<String, Object> constants = new HashMap<>();
      if(pipelineConf != null && pipelineConf.getConfiguration() != null) {
        for (ConfigConfiguration configConfiguration : pipelineConf.getConfiguration()) {
          if (configConfiguration.getName().equals("constants") && configConfiguration.getValue() != null) {
            List<Map<String, String>> consts = (List<Map<String, String>>) configConfiguration.getValue();
            for (Map<String, String> constant : consts) {
              constants.put(constant.get("key"), constant.get("value"));
            }
            return constants;
          }
        }
      }
      return constants;
    }

    @SuppressWarnings("unchecked")
    private void configureStage(StageDefinition stageDef, StageConfiguration stageConf, Class klass, Stage stage)
        throws PipelineRuntimeException {
      for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
        ConfigConfiguration confConf = stageConf.getConfig(confDef.getName());
        if (confConf == null) {
          throw new PipelineRuntimeException(ContainerError.CONTAINER_0153,
                                             stageDef.getClassName(), stageConf.getInstanceName(), confDef.getName());
        }
        Object value = confConf.getValue();
        String instanceVar = confDef.getFieldName();
        switch (confDef.getName()) {
          case ConfigDefinition.REQUIRED_FIELDS:
            requiredFields = (List<String>) value;
            break;
          case ConfigDefinition.ON_RECORD_ERROR:
            onRecordError = OnRecordError.valueOf(value.toString());
            break;
          default:
            try {
              Field var = klass.getField(instanceVar);
              //check if the field is an enum and convert the value to enum if so
              if (confDef.getModel() != null && confDef.getModel().getModelType() == ModelType.COMPLEX_FIELD) {
                setComplexField(var, stageDef, stageConf, stage, confDef, value);
              } else {
                setField(var, stageDef, stageConf, stage, confDef, value);
              }
            } catch (PipelineRuntimeException ex) {
              throw ex;
            } catch (Exception ex) {
              throw new PipelineRuntimeException(ContainerError.CONTAINER_0152,
                                                 stageDef.getClassName(), stageConf.getInstanceName(), instanceVar,
                                                 value, ex.getMessage(), ex);
            }
        }
      }
    }

    private void setComplexField(Field var, StageDefinition stageDef, StageConfiguration stageConf, Stage stage,
                                 ConfigDefinition confDef, Object value)
      throws IllegalAccessException, InstantiationException, NoSuchFieldException, PipelineRuntimeException {
      Type genericType = var.getGenericType();
      Class klass;
      if(genericType instanceof ParameterizedType) {
        Type[] typeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
        klass = (Class) typeArguments[0];
      } else {
        klass = (Class) genericType;
      }

      //value is a list of map where each map has keys which represent the field names in custom config object and the
      // value is the value of the field
      if(value instanceof List) {
        List<Object> customConfigObjects = new ArrayList<>();

        for(Object object : (List) value) {
          if(object instanceof Map) {
            Map<String, ?> map = (Map) object;
            Object customConfigObj = klass.newInstance();
            customConfigObjects.add(customConfigObj);
            for (ConfigDefinition configDefinition : confDef.getModel().getConfigDefinitions()) {
              Field f = klass.getField(configDefinition.getFieldName());
              setField(f, stageDef, stageConf, customConfigObj, configDefinition,
                       map.get(configDefinition.getFieldName()));
            }
          }
        }
        var.set(stage, customConfigObjects);
      }
    }

    private void setField(Field var, StageDefinition stageDef, StageConfiguration stageConf, Object stage,
        ConfigDefinition confDef, Object value) throws IllegalAccessException, PipelineRuntimeException {
      if(var.getType().isEnum()) {
        var.set(stage, Enum.valueOf((Class<Enum>) var.getType(), (String) value));
      } else {
        if (value != null) {
          if (value instanceof List) {
            if (confDef.getType() == ConfigDef.Type.LIST) {
              validateList((List) value, stageDef.getClassName(), stageConf.getInstanceName(), confDef.getName());
            } else if (confDef.getType() == ConfigDef.Type.MAP) {
              // special type of Map config where is a List of Maps with 'key' & 'value' entries.
              validateMapAsList((List) value, stageDef.getClassName(), stageConf.getInstanceName(),
                confDef.getName());
              value = convertToMap((List<Map>) value);
            }
          } else if (value instanceof Map) {
            validateMap((Map) value, stageDef.getClassName(), stageConf.getInstanceName(), confDef.getName());
          } else if (confDef.getType() == ConfigDef.Type.CHARACTER) {
            value = ((String)value).charAt(0);
          }
        }
        if (value != null) {
          var.set(stage, value);
        } else {
          // if the value is NULL we set the default value
          var.set(stage, confDef.getDefaultValue());
        }
      }
    }

  }

  private static void validateMapAsList(List list, String stageName, String instanceName, String configName)
      throws PipelineRuntimeException {
    for (Map map : (List<Map>) list) {
      String key = (String) map.get("key");
      String value = (String) map.get("value");
      if (!(key instanceof String) || !(value instanceof String)) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0164, stageName, instanceName, configName);
      }
    }
  }

  private static Map convertToMap(List<Map> list) {
    Map<String, String> map = new HashMap<>();
    for (Map element : list) {
      String key = (String) element.get("key");
      String value = (String) element.get("value");
      map.put(key, value);
    }
    return map;
  }

  private static void validateList(List list, String stageName, String instanceName, String configName)
      throws PipelineRuntimeException {
    for (Object e : list) {
      if (e != null && !(e instanceof String)) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0161, stageName, instanceName, configName);
      }
    }
  }

  private static void validateMap(Map map, String stageName, String instanceName, String configName)
      throws PipelineRuntimeException {
    for (Map.Entry e : ((Map<?, ?>) map).entrySet()) {
      if (!(e.getKey() instanceof String)) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0162, stageName, instanceName, configName);
      }
      if (e.getValue() != null &&  !(e.getValue() instanceof String)) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0163, stageName, instanceName, configName);
      }
    }
  }

  public Map<String, Object> getConstants() {
    return constants;
  }

  @Override
  public String toString() {
    return Utils.format("StageRuntime[{}]", getInfo());
  }

}
