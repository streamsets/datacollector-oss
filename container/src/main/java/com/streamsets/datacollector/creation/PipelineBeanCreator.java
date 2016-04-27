/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.creation;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.definition.ConfigValueExtractor;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.stagelibrary.ClassLoaderReleaser;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class PipelineBeanCreator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineBeanCreator.class);
  public static final String PIPELINE_LIB_DEFINITION = "Pipeline";

  private static final PipelineBeanCreator CREATOR = new PipelineBeanCreator() {
  };

  public static PipelineBeanCreator get() {
    return CREATOR;
  }

  public static final StageDefinition PIPELINE_DEFINITION = getPipelineDefinition();

  static private StageDefinition getPipelineDefinition() {
    StageLibraryDefinition libraryDef = new StageLibraryDefinition(Thread.currentThread().getContextClassLoader(),
                                                                   PIPELINE_LIB_DEFINITION, PIPELINE_LIB_DEFINITION, new Properties(), null, null,
                                                                   null);
    return StageDefinitionExtractor.get().extract(libraryDef, PipelineConfigBean.class, "Pipeline Config Definitions");
  }

  public PipelineConfigBean create(PipelineConfiguration pipelineConf, List<Issue> errors) {
    int priorErrors = errors.size();
    PipelineConfigBean pipelineConfigBean = createPipelineConfigs(pipelineConf, errors);
    return (errors.size() == priorErrors) ? pipelineConfigBean : null;
  }

  public PipelineBean create(boolean forExecution, StageLibraryTask library, PipelineConfiguration pipelineConf,
      List<Issue> errors) {
    int priorErrors = errors.size();
    PipelineConfigBean pipelineConfigBean = create(pipelineConf, errors);
    StageBean errorStageBean = null;
    StageBean statsStageBean = null;
    List<StageBean> stages = new ArrayList<>();
    if (pipelineConfigBean != null && pipelineConfigBean.constants != null) {
      for (StageConfiguration stageConf : pipelineConf.getStages()) {
        StageBean stageBean = createStageBean(forExecution, library, stageConf, false, pipelineConfigBean.constants,
                                              errors);
        if (stageBean != null) {
          stages.add(stageBean);
        }
      }

      StageConfiguration statsStageConf = pipelineConf.getStatsAggregatorStage();
      if (statsStageConf != null) {
        statsStageBean = createStageBean(
            forExecution,
            library,
            statsStageConf,
            false,
            pipelineConfigBean.constants,
            errors
        );
        // It is not mandatory to have a stats aggregating target configured
      }

      StageConfiguration errorStageConf = pipelineConf.getErrorStage();
      if (errorStageConf != null) {
        errorStageBean = createStageBean(forExecution, library, errorStageConf, true, pipelineConfigBean.constants,
                                         errors);
      } else {
        errors.add(IssueCreator.getPipeline().create(PipelineGroups.BAD_RECORDS.name(), "badRecordsHandling",
                                                     CreationError.CREATION_009));
      }
    }
    return (errors.size() == priorErrors) ?
        new PipelineBean(pipelineConfigBean, stages, errorStageBean, statsStageBean) : null;
  }

  public ExecutionMode getExecutionMode(PipelineConfiguration pipelineConf, List<Issue> errors) {
    ExecutionMode mode = null;
    String value = null;
    if (pipelineConf.getConfiguration("executionMode") != null) {
      if (pipelineConf.getConfiguration("executionMode").getValue() != null) {
        value = pipelineConf.getConfiguration("executionMode").getValue().toString();
      }
    }
    if (value != null) {
      try {
        mode = ExecutionMode.valueOf(value);
      } catch (IllegalArgumentException ex) {
        errors.add(IssueCreator.getPipeline().create("", "executionMode", CreationError.CREATION_070, value));
      }
    } else {
      errors.add(IssueCreator.getPipeline().create("", "executionMode", CreationError.CREATION_071));
    }
    return mode;
  }

  public String getMesosDispatcherURL(PipelineConfiguration pipelineConf) {
    String value = null;
    if (pipelineConf.getConfiguration("mesosDispatcherURL") != null) {
      value = pipelineConf.getConfiguration("mesosDispatcherURL").getValue().toString();
    }
    return value;
  }

  public String getHdfsS3ConfDirectory(PipelineConfiguration pipelineConf) {
    String value = null;
    if (pipelineConf.getConfiguration("hdfsS3ConfDir") != null) {
      value = pipelineConf.getConfiguration("hdfsS3ConfDir").getValue().toString();
    }
    return value;
  }

  StageBean createStageBean(boolean forExecution, StageLibraryTask library, StageConfiguration stageConf,
      boolean errorStage, Map<String, Object> constants, List<Issue> errors) {
    IssueCreator issueCreator = IssueCreator.getStage(stageConf.getInstanceName());
    StageBean bean = null;
    StageDefinition stageDef = library.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                forExecution);
    if (stageDef != null) {
      if (stageDef.isErrorStage() != errorStage) {
        if (stageDef.isErrorStage()) {
          errors.add(issueCreator.create(CreationError.CREATION_007, stageDef.getLibraryLabel(), stageDef.getLabel(),
                                         stageConf.getStageVersion()));
        } else {
          errors.add(issueCreator.create(CreationError.CREATION_008, stageDef.getLibraryLabel(), stageDef.getLabel(),
                                         stageConf.getStageVersion()));
        }
      }
      bean = createStage(stageDef, library, stageConf, constants, errors);
    } else {
      errors.add(issueCreator.create(CreationError.CREATION_006, stageConf.getLibrary(), stageConf.getStageName(),
                                     stageConf.getStageVersion()));
    }
    return bean;
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration getPipelineConfAsStageConf(PipelineConfiguration pipelineConf) {
    return new StageConfiguration(null, "none", "pipeline", pipelineConf.getVersion(), pipelineConf.getConfiguration(),
                                  Collections.EMPTY_MAP, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
  }

  @SuppressWarnings("unchecked")
  PipelineConfigBean createPipelineConfigs(PipelineConfiguration pipelineConf, List<Issue> errors) {
    PipelineConfigBean pipelineConfigBean = new PipelineConfigBean();
    if (createConfigBeans(pipelineConfigBean, "", PIPELINE_DEFINITION, "pipeline", errors)) {
      injectConfigs(pipelineConfigBean, "", PIPELINE_DEFINITION.getConfigDefinitionsMap(), PIPELINE_DEFINITION,
                    getPipelineConfAsStageConf(pipelineConf), Collections.EMPTY_MAP, errors);
    }
    return pipelineConfigBean;
  }

  // if not null it is OK. if null there was at least one error, check errors for the details
  StageBean createStage(StageDefinition stageDef, ClassLoaderReleaser classLoaderReleaser, StageConfiguration stageConf,
      Map<String, Object> pipelineConstants, List<Issue> errors) {
    Stage stage;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(stageDef.getStageClassLoader());
      stage = createStageInstance(stageDef, stageConf.getInstanceName(), errors);
      if (stage != null) {
        injectStageConfigs(stage, stageDef, stageConf, pipelineConstants, errors);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    StageConfigBean stageConfigBean = createAndInjectStageBeanConfigs(stageDef, stageConf, pipelineConstants, errors);
    return (errors.isEmpty()) ? new StageBean(stageDef, stageConf, stageConfigBean, stage, classLoaderReleaser) : null;
  }

  Stage createStageInstance(StageDefinition stageDef, String stageName, List<Issue> errors) {
    Stage stage = null;
    try {
      stage = (Stage) stageDef.getStageClass().newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      IssueCreator issueCreator = IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(CreationError.CREATION_000, stageDef.getLabel(), ex.toString()));
    }
    return stage;
  }

  Stage injectStageConfigs(Stage stage, StageDefinition stageDef, StageConfiguration stageConf,
      Map<String, Object> pipelineConstants, List<Issue> errors) {
    if (createConfigBeans(stage, "", stageDef, stageConf.getInstanceName(), errors)) {
      injectConfigs(stage, "", stageDef.getConfigDefinitionsMap(), stageDef, stageConf, pipelineConstants, errors);
    }
    return stage;
  }

  StageConfigBean createAndInjectStageBeanConfigs(StageDefinition stageDef, StageConfiguration stageConf,
      Map<String, Object> pipelineConstants, List<Issue> errors) {
    StageConfigBean stageConfigBean = new StageConfigBean();
    if (createConfigBeans(stageConfigBean, "", stageDef, stageConf.getInstanceName(), errors)) {
      //we use the stageDef configdefs because they may hide system configs
      injectConfigs(stageConfigBean, "", stageDef.getConfigDefinitionsMap(), stageDef, stageConf, pipelineConstants,
                    errors);
    }
    return stageConfigBean;
  }

  boolean createConfigBeans(Object obj, String configPrefix, StageDefinition stageDef, String stageName,
      List<Issue> errors) {
    boolean ok = true;
    Class klass = obj.getClass();
    for (Field field : klass.getFields()) {
      String configName = configPrefix + field.getName();
      if (field.getAnnotation(ConfigDefBean.class) != null) {
        try {
          Object bean = field.getType().newInstance();
          if (createConfigBeans(bean, configName + ".", stageDef, stageName, errors)) {
            field.set(obj, bean);
          }
        } catch (InstantiationException | IllegalAccessException ex) {
          ok = false;
          IssueCreator issueCreator = IssueCreator.getStage(stageName);
          errors.add(issueCreator.create(CreationError.CREATION_001, field.getType().getSimpleName(), ex.toString()));
        }
      }
    }
    return ok;
  }

  void injectConfigs(Object obj, Map<String, Object> valueMap, String configPrefix,
      Map<String, ConfigDefinition> configDefMap, StageDefinition stageDef, StageConfiguration stageConf,
      Map<String, Object> pipelineConstants, List<Issue> errors) {
    String stageName = stageConf.getInstanceName();
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    for (Field field : obj.getClass().getFields()) {
      String configName = configPrefix + field.getName();
      if (field.getAnnotation(ConfigDef.class) != null) {
        ConfigDefinition configDef = configDefMap.get(configName);
        if (configDef == null) {
          errors.add(issueCreator.create(configName, CreationError.CREATION_002, configName));
        } else {
          Object value = valueMap.get(configName);
          if (value == null) {
            LOG.warn("Stage '{}' missing configuration '{}', using default", stageName, configDef.getName());
            injectDefaultValue(obj, field, stageDef, stageConf, configDef, pipelineConstants, stageName, errors);
          } else {
            injectConfigValue(obj, field, value, stageDef, stageConf, configDef, null, pipelineConstants, errors);
          }
        }
      } else if (field.getAnnotation(ConfigDefBean.class) != null) {
        try {
          injectConfigs(field.get(obj), valueMap, configName + ".", configDefMap, stageDef, stageConf,
                        pipelineConstants, errors);
        } catch (IllegalArgumentException | IllegalAccessException ex) {
          errors.add(issueCreator.create(CreationError.CREATION_003, ex.toString()));
        }
      }
    }

  }

  void injectConfigs(Object obj, String configPrefix, Map<String, ConfigDefinition> configDefMap,
      StageDefinition stageDef, StageConfiguration stageConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    String stageName = stageConf.getInstanceName();
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    for (Field field : obj.getClass().getFields()) {
      String configName = configPrefix + field.getName();
      if (field.getAnnotation(ConfigDef.class) != null) {
        ConfigDefinition configDef = configDefMap.get(configName);
        // if there is no config def, we ignore it, it can be the case when the config is a @HideConfig
        if (configDef != null) {
          Config configConf = stageConf.getConfig(configName);
          if (configConf == null) {
            LOG.warn("Stage '{}' missing configuration '{}', using default", stageName, configDef.getName());
            injectDefaultValue(obj, field, stageDef, stageConf, configDef, pipelineConstants, stageName, errors);
          } else {
            injectConfigValue(obj, field, stageDef, stageConf, configDef, configConf, pipelineConstants, errors);
          }
        }
      } else if (field.getAnnotation(ConfigDefBean.class) != null) {
        try {
          injectConfigs(field.get(obj), configName + ".", configDefMap, stageDef, stageConf, pipelineConstants, errors);
        } catch (IllegalArgumentException | IllegalAccessException ex) {
          errors.add(issueCreator.create(CreationError.CREATION_003, ex.toString()));
        }
      }
    }
  }

  void injectDefaultValue(Object obj, Field field, StageDefinition stageDef, StageConfiguration stageConf,
      ConfigDefinition configDef, Map<String, Object> pipelineConstants, String stageName, List<Issue> errors) {
    Object defaultValue = configDef.getDefaultValue();
    if (defaultValue != null) {
      injectConfigValue(obj, field, defaultValue, stageDef, stageConf, configDef, null, pipelineConstants, errors);
    } else if (!hasJavaDefault(obj, field)) {
      defaultValue = configDef.getType().getDefault(field.getType());
      injectConfigValue(obj, field, defaultValue, stageDef, stageConf, configDef, null, pipelineConstants, errors);
    }
  }

  boolean hasJavaDefault(Object obj, Field field) {
    try {
      return field.get(obj) != null;
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
    }
  }

  Object toEnum(Class klass, Object value, StageDefinition stageDef, String stageName, String groupName,
      String configName, List<Issue> errors) {
    try {
      value = Enum.valueOf(klass, value.toString());
    } catch (IllegalArgumentException ex) {
      IssueCreator issueCreator = IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_010, value, klass.getSimpleName(),
                                     ex.toString()));
      value = null;
    }
    return value;
  }

  Object toString(Object value, StageDefinition stageDef, String stageName, String groupName, String configName,
      List<Issue> errors) {
    if (!(value instanceof String)) {
      IssueCreator issueCreator = IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_011, value,
                                     value.getClass().getSimpleName()));
      value = null;
    }
    return value;
  }

  Object toChar(Object value, StageDefinition stageDef, String stageName, String groupName, String configName,
      List<Issue> errors) {
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    if (value instanceof String) {
      String strValue = value.toString();
      if (!strValue.isEmpty() && strValue.startsWith("\\u") && strValue.length() > 5 &&
          strValue.substring(2).matches("^[0-9a-fA-F]+$")) {
        // To support non printable unicode control characters
        value = (char) Integer.parseInt(strValue.substring(2), 16 );
      } else if (strValue.isEmpty() || strValue.length() > 1) {
        errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_012, value, strValue));
        value = null;
      } else {
        value = strValue.charAt(0);
      }
    } else if (!(value instanceof Character)) {
      String valueType = value == null ? "null" : value.getClass().getName();
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_012, value, valueType));
      value = null;
    }
    return value;
  }

  Object toBoolean(Object value, StageDefinition stageDef, String stageName, String groupName, String configName,
      List<Issue> errors) {
    if (!(value instanceof Boolean)) {
      IssueCreator issueCreator = IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_013, value));
      value = null;
    }
    return value;
  }

  private static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPER_MAP
      = new ImmutableMap.Builder<Class<?>, Class<?>>()
      .put(byte.class, Byte.class)
      .put(short.class, Short.class)
      .put(int.class, Integer.class)
      .put(long.class, Long.class)
      .put(float.class, Float.class)
      .put(double.class, Double.class)
      .build();

  private static final Map<Class<?>, Method> WRAPPERS_VALUE_OF_MAP = new HashMap<>();

  @SuppressWarnings("unchecked")
  private static Method getValueOfMethod(Class klass) {
    try {
      return klass.getMethod("valueOf", String.class);
    } catch (Exception ex)  {
      throw new RuntimeException(ex);
    }
  }

  static {
    for (Class klass : PRIMITIVE_WRAPPER_MAP.values()) {
      WRAPPERS_VALUE_OF_MAP.put(klass, getValueOfMethod(klass));
    }
  }

  Object toNumber(Class numberType, Object value, StageDefinition stageDef, String stageName, String groupName,
      String configName, List<Issue> errors) {
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    if (!ConfigValueExtractor.NUMBER_TYPES.contains(value.getClass())) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_014, value));
      value = null;
    } else {
      try {
        if (PRIMITIVE_WRAPPER_MAP.containsKey(numberType)) {
          numberType = PRIMITIVE_WRAPPER_MAP.get(numberType);
        }
        value = WRAPPERS_VALUE_OF_MAP.get(numberType).invoke(null, value.toString());
      } catch (Exception ex) {
        errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_015, value,
                                       numberType.getSimpleName(), ex.toString()));
        value = null;
      }
    }
    return value;
  }

  Object toList(Object value, StageDefinition stageDef, ConfigDefinition configDef,
      Map<String, Object> pipelineConstants, String stageName, String groupName, String configName,
      List<Issue> errors, Field field) {
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    if (!(value instanceof List)) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_020));
      value = null;
    } else {
      boolean error = false;
      List<Object> list = new ArrayList<>();
      for (Object element : (List) value) {
        if (element == null) {
          errors.add(issueCreator.create(groupName, configName,  CreationError.CREATION_021));
          error = true;
        } else {
          element = resolveIfImplicitEL(element, stageDef, configDef, pipelineConstants, stageName, errors);
          if (element != null) {
            //We support list of String and enums.
            //If the field type is enum and the element is String, convert to enum
            if(field != null) {
              Type type = field.getGenericType();
              if (type instanceof ParameterizedType) {
                Type type1 = ((ParameterizedType) type).getActualTypeArguments()[0];
                if(type1 instanceof Class && ((Class<?>)type1).isEnum()) {
                  element = toEnum((Class<?>)type1, element, stageDef, stageName, groupName, configName, errors);
                }
              }
            }
            list.add(element);
          } else {
            error = true;
          }
        }
      }
      value = (error) ? null : list;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  Object toMap(Object value, StageDefinition stageDef, ConfigDefinition configDef,
      Map<String, Object> pipelineConstants, String stageName, String groupName, String configName,
      List<Issue> errors) {
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    if (!(value instanceof List)) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_030));
      value = null;
    } else {
      boolean error = false;
      Map map = new LinkedHashMap();
      for (Object entry : (List) value) {
        if (!(entry instanceof Map)) {
          error = true;
          errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_031,
                                         entry.getClass().getSimpleName()));
        } else {

          Object k = ((Map)entry).get("key");
          if (k == null) {
            errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_032));
          }

          Object v = ((Map)entry).get("value");
          if (v == null) {
            errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_033));
          } else {
            v = resolveIfImplicitEL(v, stageDef, configDef, pipelineConstants, stageName, errors);
          }

          if (k != null && v != null) {
            map.put(k, v);
          } else {
            error = true;
          }
        }
      }
      value = (error) ? null : map;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  Object toComplexField(Object value, StageDefinition stageDef, StageConfiguration stageConf,
      ConfigDefinition configDef, Config configConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    String stageName = stageConf.getInstanceName();
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    if (!(value instanceof List)) {
      errors.add(issueCreator.create(configDef.getGroup(), configDef.getName(), CreationError.CREATION_040,
                           value.getClass().getSimpleName()));
      value = null;
    } else {
      boolean error = false;
      List<Object> list = new ArrayList<>();
      try {
        // we need to use the classloader fo the stage to instatiate the ComplexField so if the stage has a private
        // classloader we use the same one.
        Class klass = Thread.currentThread().getContextClassLoader()
                            .loadClass(configDef.getModel().getListBeanClass().getName());
        List listValue = (List) value;
        for (int i = 0; i < listValue.size(); i++) {
          Map<String, Object> configElement;
          try {
            configElement = (Map<String, Object>) listValue.get(i);
            try {
              Object element = klass.newInstance();
              if (createConfigBeans(element, configDef.getName() + ".", stageDef, stageConf.getInstanceName(), errors)) {
                injectConfigs(element, configElement, "", configDef.getModel().getConfigDefinitionsAsMap(), stageDef,
                              stageConf, pipelineConstants, errors);
                list.add(element);
              }
            } catch (InstantiationException | IllegalAccessException ex) {
              errors.add(issueCreator.create(configDef.getGroup(), Utils.format("{}[{}]", configConf.getName(), i),
                                             CreationError.CREATION_041, klass.getSimpleName(), ex.toString()));
              error = true;
              break;
            }
          } catch (ClassCastException ex) {
            errors.add(issueCreator.create(configDef.getGroup(), Utils.format("{}[{}]", configConf.getName(), i),
                                           CreationError.CREATION_042, ex.toString()));
          }
        }
        value = (error) ? null : list;
      } catch (ClassNotFoundException ex) {
        value = null;
        errors.add(issueCreator.create(configDef.getGroup(), configConf.getName(), CreationError.CREATION_043,
                                       ex.toString()));
      }
    }
    return value;
  }

  Object resolveIfImplicitEL(Object value, StageDefinition stageDef, ConfigDefinition configDef,
      Map<String, Object> pipelineConstants, String stageName, List<Issue> errors) {
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    if (configDef.getEvaluation() == ConfigDef.Evaluation.IMPLICIT && value instanceof String &&
        ElUtil.isElString(value)) {
      try {
        value = ElUtil.evaluate(value, stageDef, configDef, pipelineConstants);
      } catch (ELEvalException ex) {
        errors.add(issueCreator.create(configDef.getGroup(), configDef.getName(), CreationError.CREATION_005,
                                       value, ex.toString()));
        value = null;
      }
    }
    return value;
  }

  void injectConfigValue(Object obj, Field field, StageDefinition stageDef, StageConfiguration stageConf,
      ConfigDefinition configDef, Config configConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    Object value = configConf.getValue();
    if (value == null) {
      injectDefaultValue(obj, field, stageDef, stageConf, configDef, pipelineConstants, stageConf.getInstanceName(),
                         errors);
    } else {
      injectConfigValue(obj, field, value, stageDef, stageConf, configDef, configConf, pipelineConstants, errors);
    }
  }


  void injectConfigValue(Object obj, Field field, Object value, StageDefinition stageDef, StageConfiguration stageConf,
      ConfigDefinition configDef, Config configConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    String stageName = stageConf.getInstanceName();
    IssueCreator issueCreator = IssueCreator.getStage(stageName);
    String groupName = configDef.getGroup();
    String configName = configDef.getName();
    if (value == null) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_050));
    } else {
      if (configDef.getModel() != null && configDef.getModel().getModelType() == ModelType.LIST_BEAN) {
        value = toComplexField(value, stageDef, stageConf, configDef, configConf, pipelineConstants, errors);
      } else if (List.class.isAssignableFrom(field.getType())) {
        value = toList(value, stageDef, configDef, pipelineConstants, stageName, groupName, configName, errors, field);
      } else if (Map.class.isAssignableFrom(field.getType())) {
        value = toMap(value, stageDef, configDef, pipelineConstants, stageName, groupName, configName, errors);
      } else {
        value = resolveIfImplicitEL(value, stageDef, configDef, pipelineConstants, stageName, errors);
        if (value != null) {
          if (field.getType().isEnum()) {
            value = toEnum(field.getType(), value, stageDef, stageName, groupName, configName, errors);
          } else if (field.getType() == String.class) {
            value = toString(value, stageDef, stageName, groupName, configName, errors);
          } else if (List.class.isAssignableFrom(field.getType())) {
            value = toList(value, stageDef, configDef, pipelineConstants, stageName, groupName, configName, errors,
              field);
          } else if (Map.class.isAssignableFrom(field.getType())) {
            value = toMap(value, stageDef, configDef, pipelineConstants, stageName, groupName, configName, errors);
          } else if (ConfigValueExtractor.CHARACTER_TYPES.contains(field.getType())) {
            value = toChar(value, stageDef, stageName, groupName, configName, errors);
          } else if (ConfigValueExtractor.BOOLEAN_TYPES.contains(field.getType())) {
            value = toBoolean(value, stageDef, stageName, groupName, configName, errors);
          } else if (ConfigValueExtractor.NUMBER_TYPES.contains(field.getType())) {
            value = toNumber(field.getType(), value, stageDef, stageName, groupName, configName, errors);
          } else {
            errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_051,
                                           field.getType().getSimpleName()));
            value = null;
          }
        }
      }
      if (value != null) {
        try {
          field.set(obj, value);
        } catch (IllegalAccessException ex) {
          errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_060, value, ex.toString()));
        }
      }
    }
  }

}
