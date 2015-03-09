/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stagelibrary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.OnRecordErrorChooserValues;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ElFunctionDefinition;
import com.streamsets.pipeline.el.ElMetadata;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.StageDefinitionJson;
import com.streamsets.pipeline.runner.StageContext;
import com.streamsets.pipeline.task.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ClassLoaderStageLibraryTask extends AbstractTask implements StageLibraryTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderStageLibraryTask.class);

  private static final String PIPELINE_STAGES_JSON = "PipelineStages.json";

  private final RuntimeInfo runtimeInfo;
  private List<? extends ClassLoader> stageClassLoaders;
  private Map<String, StageDefinition> stageMap;
  private List<StageDefinition> stageList;
  private LoadingCache<Locale, List<StageDefinition>> localizedStageList;
  private ObjectMapper json;
  private Map<String, ElFunctionDefinition> elFunctionDefinitionMap;
  private Map<String, ElConstantDefinition> elVariableDefinitionMap;
  private Map<String, String> elGroupsMap;

  @Inject
  public ClassLoaderStageLibraryTask(RuntimeInfo runtimeInfo) {
    super("stageLibrary");
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  protected void initTask() {
    stageClassLoaders = runtimeInfo.getStageLibraryClassLoaders();
    json = ObjectMapperFactory.get();
    stageList = new ArrayList<>();
    stageMap = new HashMap<>();
    elFunctionDefinitionMap = new HashMap<>();
    elVariableDefinitionMap = new HashMap<>();
    elGroupsMap = new HashMap<>();
    loadStages();
    stageList = ImmutableList.copyOf(stageList);
    stageMap = ImmutableMap.copyOf(stageMap);

    localizedStageList = CacheBuilder.newBuilder().build(new CacheLoader<Locale, List<StageDefinition>>() {
      @Override
      public List<StageDefinition> load(Locale key) throws Exception {
        List<StageDefinition> list = new ArrayList<>();
        for (StageDefinition stage : stageList) {
          list.add(stage.localize());
        }
        return list;
      }
    });

  }

  @VisibleForTesting
  void loadStages() {
    if (LOG.isDebugEnabled()) {
      for (ClassLoader cl : stageClassLoaders) {
        LOG.debug("About to load stages from library '{}'", StageLibraryUtils.getLibraryName(cl));
      }
    }
    try {
      LocaleInContext.set(Locale.getDefault());
      for (ClassLoader cl : stageClassLoaders) {
        String libraryName = StageLibraryUtils.getLibraryName(cl);
        String libraryLabel = StageLibraryUtils.getLibraryLabel(cl);
        LOG.debug("Loading stages from library '{}'", libraryName);
        try {
          Enumeration<URL> resources = cl.getResources(PIPELINE_STAGES_JSON);
          while (resources.hasMoreElements()) {
            Map<String, String> stagesInLibrary = new HashMap<>();
            URL url = resources.nextElement();
            InputStream is = url.openStream();
            StageDefinitionJson[] stages =
              json.readValue(is, StageDefinitionJson[].class);
            for (StageDefinitionJson stageDef : stages) {
              StageDefinition stage = BeanHelper.unwrapStageDefinition(stageDef);
              stage.setLibrary(libraryName, libraryLabel, cl);
              String key = createKey(libraryName, stage.getName(), stage.getVersion());
              LOG.debug("Loaded stage '{}' (library:name:version)", key);
              if (stagesInLibrary.containsKey(key)) {
                throw new IllegalStateException(Utils.format(
                    "Library '{}' contains more than one definition for stage '{}', class '{}' and class '{}'",
                    libraryName, key, stagesInLibrary.get(key), stage.getStageClass()));
              }
              addSystemConfigurations(stage);
              stagesInLibrary.put(key, stage.getClassName());
              stageList.add(stage);
              stageMap.put(key, stage);
              convertDefaultValueToType(stage);
              convertTriggeredByValuesToType(stage.getStageClassLoader().loadClass(stage.getClassName()),
                stage.getConfigDefinitions());
              setElMetadata(stage);
            }
          }
        } catch (IOException | ClassNotFoundException | NoSuchFieldException | InvocationTargetException |
          NoSuchMethodException | InstantiationException | IllegalAccessException ex) {
          throw new RuntimeException(
              Utils.format("Could not load stages definition from '{}', {}", cl, ex.getMessage()),
              ex);
        }
      }
    } finally {
      LocaleInContext.set(null);
    }
  }

  private void setElMetadata(StageDefinition stageDefinition) throws ClassNotFoundException, IllegalAccessException,
    InstantiationException, NoSuchMethodException, InvocationTargetException {
    Class<?> stageClass = stageDefinition.getStageClassLoader().loadClass(stageDefinition.getClassName());
    Stage<?> stage = (Stage<?>) stageClass.newInstance();
    List<ELEval> elEvals = stage.getElEvals(new StageContext(stageDefinition.getName(), stageDefinition.getType(),
      false,OnRecordError.TO_ERROR,Collections.<String>emptyList()));
    for(ELEval elEval : elEvals) {
      ConfigDefinition configDefinition = stageDefinition.getConfigDefinition(elEval.getConfigName());
      List<String> elFunctionDefinitionIds = configDefinition.getElFunctionDefinitionIds();
      for(ElFunctionDefinition elFunctionDefinition : ((ELEvaluator) elEval).getElFunctionDefinitions()) {
        elFunctionDefinitionIds.add(elFunctionDefinition.getId());
        if(!elFunctionDefinitionMap.containsKey(elFunctionDefinition.getId())) {
          elFunctionDefinitionMap.put(elFunctionDefinition.getId(), elFunctionDefinition);
          if(!elGroupsMap.containsKey(elFunctionDefinition.getGroup())) {
            elGroupsMap.put(elFunctionDefinition.getGroup(), elFunctionDefinition.getGroup());
          }
        }
      }
      List<String> elVariableDefinitionIds = configDefinition.getElVariableDefinitionIds();
      for(ElConstantDefinition elConstantDefinition : ((ELEvaluator) elEval).getElConstantDefinitions()) {
        elVariableDefinitionIds.add(elConstantDefinition.getId());
        if(!elVariableDefinitionMap.containsKey(elConstantDefinition.getId())) {
          elVariableDefinitionMap.put(elConstantDefinition.getId(), elConstantDefinition);
        }
      }
    }
  }

  //Group name needs to be empty for UI to show the config in General Group.
  private static final ConfigDefinition REQUIRED_FIELDS_CONFIG = new ConfigDefinition(
      ConfigDefinition.REQUIRED_FIELDS, ConfigDef.Type.MODEL, "Required Fields",
      "Records without any of these fields are sent to error",
      null, false, "", null, new ModelDefinition(ModelType.FIELD_SELECTOR_MULTI_VALUED, null, null, null, null, null),
      "", new ArrayList<>(), 10, new ArrayList<String>(), new ArrayList<String>());

  //Group name needs to be empty for UI to show the config in General Group.
  private static final ConfigDefinition ON_RECORD_ERROR_CONFIG = new ConfigDefinition(
      ConfigDefinition.ON_RECORD_ERROR, ConfigDef.Type.MODEL, "On Record Error",
      "Action to take with records sent to error",
      OnRecordError.TO_ERROR.name(), true, "", null, new ModelDefinition(ModelType.VALUE_CHOOSER, ChooserMode.PROVIDED,
                                                 OnRecordErrorChooserValues.class.getName(),
                                                 new OnRecordErrorChooserValues().getValues(),
                                                 new OnRecordErrorChooserValues().getLabels(), null), "",
      new ArrayList<>(), 20, new ArrayList<String>(), new ArrayList<String>());

  private void addSystemConfigurations(StageDefinition stage) {
    if (stage.hasRequiredFields()) {
      stage.addConfiguration(REQUIRED_FIELDS_CONFIG);
    }
    if (stage.hasOnRecordError()) {
      stage.addConfiguration(ON_RECORD_ERROR_CONFIG);
    }
  }

  private String createKey(String library, String name, String version) {
    return library + ":" + name + ":" + version;
  }

  @Override
  public List<StageDefinition> getStages() {
    try {
      return (LocaleInContext.get() == null) ? stageList : localizedStageList.get(LocaleInContext.get());
    } catch (ExecutionException ex) {
      LOG.warn("Error loading locale '{}', {}", LocaleInContext.get(), ex.getMessage(), ex);
      return stageList;
    }
  }

  @Override
  public ElMetadata getElMetadata() {
    return new ElMetadata(elFunctionDefinitionMap, elVariableDefinitionMap, elGroupsMap);
  }

  @Override
  @SuppressWarnings("unchecked")
  public StageDefinition getStage(String library, String name, String version) {
    return stageMap.get(createKey(library, name, version));
  }

  private void convertTriggeredByValuesToType(Class<?> stageClass, List<ConfigDefinition> configDefinitions)
    throws ClassNotFoundException,
    NoSuchFieldException, IOException {
    for (ConfigDefinition confDef : configDefinitions) {
      if(confDef.getFieldName() == null) {
        //confDef.getFieldName() is null for system config definitions like required fields etc.
        continue;
      }
      if(confDef.getDependsOn() == null || confDef.getDependsOn().isEmpty()) {
        continue;
      }
      Field field = stageClass.getField(confDef.getDependsOn());
      //Complex types must be handled
      if (confDef.getModel() != null && confDef.getModel().getModelType() == ModelType.COMPLEX_FIELD) {
        Type genericType = field.getGenericType();
        Class<?> klass;
        if (genericType instanceof ParameterizedType) {
          Type[] typeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
          klass = (Class<?>) typeArguments[0];
        } else {
          klass = (Class<?>) genericType;
        }
        convertTriggeredByValuesToType(klass, confDef.getModel().getConfigDefinitions());
      } else {
        List<Object> triggeredByValues = new ArrayList<>();
        for(Object value : confDef.getTriggeredByValues()) {
          //Boolean, Int, long are converted by annotation processor, so no-op here
          //Enum can be left as String
          //Only Map and List types are converted from json string to map and List.
          if(value instanceof String &&
            (field.getType().isAssignableFrom(List.class) || field.getType().isAssignableFrom(Map.class))) {
            String valueString = (String) value;
            if (valueString != null && !valueString.isEmpty()) {
              //the defaultValue string is treated as JSON
              //Because of UI limitations both List and Map must be serialized and de-serialized as List.
              //So Map with 2 entries will be converted 2 Maps.
              // Each Map has 2 entries
              // "key"      ->     <key in the original map>
              //  "value"   ->     <value in the original map>
              triggeredByValues.add(ObjectMapperFactory.get().readValue(valueString, List.class));
            }
          } else {
            triggeredByValues.add(value);
          }
        }
        confDef.setTriggeredByValues(triggeredByValues);
      }
    }
  }

  private void convertDefaultValueToType(StageDefinition stageDefinition) throws ClassNotFoundException,
    NoSuchFieldException, IOException {
    Class<?> stageClass = stageDefinition.getStageClassLoader().loadClass(stageDefinition.getClassName());
    for (ConfigDefinition confDef : stageDefinition.getConfigDefinitions()) {
      if(confDef.getFieldName() == null) {
        //confDef.getFieldName() is null for system config definitions like required fields etc.
        continue;
      }
      Field field = stageClass.getField(confDef.getFieldName());
      //Complex types must be handled
      if (confDef.getModel() != null && confDef.getModel().getModelType() == ModelType.COMPLEX_FIELD) {
        setDefaultForComplexType(field, confDef);
      }
      setDefaultForConfigDef(confDef, field);
    }
  }

  private void setDefaultForComplexType(Field field, ConfigDefinition confDef)
    throws NoSuchFieldException, IOException {
    Type genericType = field.getGenericType();
    Class<?> klass;
    if (genericType instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
      klass = (Class<?>) typeArguments[0];
    } else {
      klass = (Class<?>) genericType;
    }
    for (ConfigDefinition configDefinition : confDef.getModel().getConfigDefinitions()) {
      Field f = klass.getField(configDefinition.getFieldName());
      setDefaultForConfigDef(configDefinition, f);
    }
  }

  private void setDefaultForConfigDef(ConfigDefinition configDef, Field field) throws IOException {
    if(!(configDef.getDefaultValue() instanceof String)) {
      return;
    }
    Object defaultValue = null;
    String defaultValueString = (String) configDef.getDefaultValue();
    if (defaultValueString != null && !defaultValueString.isEmpty()) {
      //Boolean, Int, long are converted by annotation processor
      //Enum can be left as String
      //Map and classes are converted from json string to map and List.

      if (field.getType().isAssignableFrom(List.class) || field.getType().isAssignableFrom(Map.class)) {
        //the defaultValue string is treated as JSON
        //Because of UI limitations both List and Map must be serialized and de-serialized as List.
        //So Map with 2 entries will be converted 2 Maps.
        // Each Map has 2 entries
        // "key"      ->     <key in the original map>
        //  "value"   ->     <value in the original map>
        defaultValue = ObjectMapperFactory.get().readValue(defaultValueString, List.class);
      }
      if (defaultValue != null) {
        configDef.setDefaultValue(defaultValue);
      }
    }
  }
}
