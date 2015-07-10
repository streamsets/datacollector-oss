/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.annotations.VisibleForTesting;
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
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.CreateByRef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.ElUtil;
import com.streamsets.pipeline.util.ValidationUtil;
import com.streamsets.pipeline.validation.Issue;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class StageRuntime {
  private final StageDefinition def;
  private final StageConfiguration conf;
  private final Stage stage;
  private final Stage.Info info;
  private final List<String> requiredFields;
  private final List<String> preconditions;
  private final OnRecordError onRecordError;
  private StageContext context;
  private final Map<String, Object> constants;

  @VisibleForTesting
  StageRuntime(final StageDefinition def, final StageConfiguration conf, List<String> requiredFields,
      List<String> preconditions, OnRecordError onRecordError, Stage stage, Map<String, Object> constants) {
    this.def = def;
    this.conf = conf;
    this.requiredFields = requiredFields;
    this.preconditions = preconditions;
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

  public List<String> getPreconditions() {
    return preconditions;
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
  public List<Issue> validateConfigs() throws StageException {
    //TODO: for errorstage we must set the errorstage flag in issues, use IssueCreator.getErrorStage()
    Preconditions.checkState(context != null, "context has not been set");
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getDefinition().getStageClassLoader());
      List<Issue> issues = stage.validateConfigs(info, context);
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
    private List<String> preconditions;
    private OnRecordError onRecordError;

    public Builder(StageLibraryTask stageLib, String name, PipelineConfiguration pipelineConf) {
      this.stageLib = stageLib;
      this.name = name;
      this.pipelineConf = pipelineConf;
      onRecordError = OnRecordError.STOP_PIPELINE;
    }

    public StageRuntime[] build() throws PipelineRuntimeException {
      PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLib, name, pipelineConf, true);
      if (!validator.validate()) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0150, ValidationUtil.getFirstIssueAsString(name,
          validator.getIssues()));
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
        Map<String, Object> constants = ElUtil.getConstants(pipelineConf);
        configureStage(def, conf, klass, stage, constants);
        return new StageRuntime(def, conf, requiredFields, preconditions, onRecordError, stage, constants);
      } catch (Exception ex) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0151, ex.getMessage(), ex);
      }
    }

    @SuppressWarnings("unchecked")
    private void configureStage(StageDefinition stageDef, StageConfiguration stageConf, Class klass, Stage stage,
                                Map<String, Object> constants)
        throws PipelineRuntimeException {
      List<ConfigConfiguration> newConfigToAdd = new ArrayList<>();
      for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
        ConfigConfiguration confConf = stageConf.getConfig(confDef.getName());
        if (confConf == null) {
          confConf = new ConfigConfiguration(confDef.getName(), confDef.getDefaultValue());
          newConfigToAdd.add(confConf);
        }
        Object value = confConf.getValue();
        String instanceVar = confDef.getFieldName();
        switch (confDef.getName()) {
          case ConfigDefinition.REQUIRED_FIELDS:
            requiredFields = (List<String>) value;
            break;
          case ConfigDefinition.PRECONDITIONS:
            preconditions = (List<String>) value;
            break;
          case ConfigDefinition.ON_RECORD_ERROR:
            onRecordError = OnRecordError.valueOf(value.toString());
            break;
          default:
            try {
              Field var = klass.getField(instanceVar);
              //check if the field is an enum and convert the value to enum if so
              if (confDef.getModel() != null && confDef.getModel().getModelType() == ModelType.COMPLEX_FIELD) {
                setComplexField(var, stageDef, stageConf, stage, confDef, value, constants);
              } else {
                setField(var, stageDef, stageConf, stage, confDef, value, constants);
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
      if(!newConfigToAdd.isEmpty()) {
        newConfigToAdd.addAll(stageConf.getConfiguration());
        stageConf.setConfig(newConfigToAdd);
      }
    }

    private void setComplexField(Field var, StageDefinition stageDef, StageConfiguration stageConf, Stage stage,
                                 ConfigDefinition confDef, Object value, Map<String, Object> constants)
      throws IllegalAccessException, InstantiationException, NoSuchFieldException, PipelineRuntimeException, ELEvalException, ClassNotFoundException {
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
                       map.get(configDefinition.getFieldName()), constants);
            }
          }
        }
        var.set(stage, customConfigObjects);
      }
    }

    private void setField(Field var, StageDefinition stageDef, StageConfiguration stageConf, Object stage,
        ConfigDefinition confDef, Object value, Map<String, Object> constants)
      throws IllegalAccessException, PipelineRuntimeException, ELEvalException, ClassNotFoundException {
      if(var.getType().isEnum()) {
        var.set(stage, Enum.valueOf((Class<Enum>) var.getType(), (String) value));
      } else {
        if (value != null) {
          if (value instanceof List) {
            if (confDef.getType() == ConfigDef.Type.LIST) {
              value = validateList((List)value, var, constants, stageDef, stageConf.getInstanceName(), confDef);
            } else if (confDef.getType() == ConfigDef.Type.MAP) {
              // special type of Map config where is a List of Maps with 'key' & 'value' entries.
              validateMapAsList((List) value, stageDef.getClassName(), stageConf.getInstanceName(),
                confDef.getName());
              value = convertToMap((List<Map>) value, var, constants, stageDef, stageConf.getInstanceName(), confDef);
            }
          } else if (value instanceof Map) {
            value = validateMap((Map) value, var, constants, stageDef, stageConf.getInstanceName(), confDef);
          } else if (confDef.getType() == ConfigDef.Type.CHARACTER) {
            //could be an el expression which evaluates to character
            String stringVal = (String) value;
            if(stringVal.startsWith("${")) {
              Object evaluatedValue = ElUtil.evaluate(value, stageDef, confDef, constants);
              if(evaluatedValue instanceof Character) {
                value = evaluatedValue;
              } else if (evaluatedValue instanceof String) {
                value = ((String) evaluatedValue).charAt(0);
              } else {
                //not character and not of type String => exception
                throw new PipelineRuntimeException(ContainerError.CONTAINER_0152, stageDef.getName(),
                  stageConf.getInstanceName(), confDef.getName(), value);
              }
            } else {
              value = stringVal.charAt(0);
            }
          } else if (ElUtil.isElString(value)) {
            value = ElUtil.evaluate(value, stageDef, confDef, constants);
          }
        }
        if (value != null) {
          var.set(stage, value);
        } else {
          // if the value is NULL we set the default value
          //default value could be el String
          Object defaultValue = confDef.getDefaultValue();
          if(ElUtil.isElString(defaultValue)) {
            var.set(stage, ElUtil.evaluate(defaultValue, stageDef, confDef, constants));
          } else {
            var.set(stage, defaultValue);
          }
        }
      }
    }

  }

  private static void validateMapAsList(List list, String stageName, String instanceName, String configName)
      throws PipelineRuntimeException {
    for (Map<String, String> map : (List<Map>) list) {
      if (!map.isEmpty()) {
        String key = map.get("key");
        String value = map.get("value");
        if (!(key instanceof String) || !(value instanceof String)) {
          throw new PipelineRuntimeException(ContainerError.CONTAINER_0164, stageName, instanceName, configName);
        }
      }
    }
  }

  private static Map convertToMap(List<Map> list, Field var, Map<String, Object> constants,
                                  StageDefinition stageDef, String instanceName, ConfigDefinition confDef)
    throws PipelineRuntimeException, ELEvalException, ClassNotFoundException {
    Map<String, String> map = new HashMap<>();
    for (Map element : list) {
      if (!element.isEmpty()) {
        String key = (String) element.get("key");
        String value = (String) element.get("value");
        Object evaluatedValue = ElUtil.evaluate(value, stageDef, confDef, constants);
        checkForString(evaluatedValue, stageDef.getName(), instanceName, confDef.getName());
        map.put(key, (String) evaluatedValue);
      }
    }
    return map;
  }

  private static List<String> validateList(List list, Field var, Map<String, Object> constants,
                                           StageDefinition stageDef, String instanceName, ConfigDefinition confDef)
    throws PipelineRuntimeException, ELEvalException, ClassNotFoundException {
    List<String> evaluatedValues = new ArrayList<>();
    for (Object e : list) {
      checkForString(e, stageDef.getName(), instanceName, confDef.getName());
      String value = (String)e;
      Object evaluatedValue = ElUtil.evaluate(value, stageDef, confDef, constants);
      checkForString(evaluatedValue, stageDef.getName(), instanceName, confDef.getName());
      evaluatedValues.add((String) evaluatedValue);
    }
    return evaluatedValues;
  }

  private static Map<String, String> validateMap(Map map, Field var, Map<String, Object> constants,
                                                 StageDefinition stageDef, String instanceName, ConfigDefinition confDef)
    throws PipelineRuntimeException, ELEvalException, ClassNotFoundException {
    Map<String, String> evaluatedMap = new HashMap<>();
    for (Map.Entry e : ((Map<?, ?>) map).entrySet()) {
      if (!(e.getKey() instanceof String)) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0162, stageDef.getName(), instanceName,
          confDef.getName());
      }
      checkForString(e.getValue(), stageDef.getName(), instanceName, confDef.getName());
      String value = (String)e.getValue();
      Object evaluatedValue = ElUtil.evaluate(value, stageDef, confDef, constants);
      checkForString(evaluatedValue, stageDef.getName(), instanceName, confDef.getName());
      evaluatedMap.put((String) e.getKey(), (String) evaluatedValue);
    }
    return evaluatedMap;
  }

  private static void checkForString(Object e, String stageName, String instanceName, String configName)
    throws PipelineRuntimeException {
    if (e != null && !(e instanceof String)) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0161, stageName, instanceName, configName);
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
