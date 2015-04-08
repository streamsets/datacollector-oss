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
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.OnRecordErrorChooserValues;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ErrorHandlingChooserValues;
import com.streamsets.pipeline.config.ModelDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;
import com.streamsets.pipeline.el.RuntimeEL;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.StageDefinitionJson;
import com.streamsets.pipeline.task.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
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
    loadStages();
    stageList = ImmutableList.copyOf(stageList);
    stageMap = ImmutableMap.copyOf(stageMap);

    // localization cache for definitions
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

    // initializing the list of targets that can be used for error handling
    ErrorHandlingChooserValues.setErrorHandlingOptions(this);

  }

  @VisibleForTesting
  void loadStages() {
    if (LOG.isDebugEnabled()) {
      for (ClassLoader cl : stageClassLoaders) {
        LOG.debug("About to load stages from library '{}'", StageLibraryUtils.getLibraryName(cl));
      }
    }

    try {
      RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    } catch (IOException e) {
      throw new RuntimeException(
        Utils.format("Could not load runtime configuration, '{}'", e.getMessage()), e);
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
              computeDependsOnChain(stage);
              setElMetadata(stage);
            }
          }
        } catch (IOException | ClassNotFoundException | NoSuchFieldException | InstantiationException
          | IllegalAccessException ex) {
          throw new RuntimeException(
              Utils.format("Could not load stages definition from '{}', {}", cl, ex.getMessage()),
              ex);
        }
      }
    } finally {
      LocaleInContext.set(null);
    }
  }

  private void computeDependsOnChain(StageDefinition stageDefinition) {
    Map<String, ConfigDefinition> configDefinitionsMap = stageDefinition.getConfigDefinitionsMap();
    for(Map.Entry<String, ConfigDefinition> entry :  configDefinitionsMap.entrySet()) {
      ConfigDefinition configDef = entry.getValue();
      ConfigDefinition tempConfigDef = configDef;
      Map<String, List<Object>> dependsOnMap = new HashMap<>();
      while(tempConfigDef != null &&
        tempConfigDef.getDependsOn() != null &&
        !tempConfigDef.getDependsOn().isEmpty()) {

        dependsOnMap.put(tempConfigDef.getDependsOn(), tempConfigDef.getTriggeredByValues());
        tempConfigDef = configDefinitionsMap.get(tempConfigDef.getDependsOn());
      }
      if(dependsOnMap.isEmpty()) {
        //Request from UI to set null for efficiency
        configDef.setDependsOnMap(null);
      } else {
        configDef.setDependsOnMap(dependsOnMap);
      }
    }
  }

  //Group name needs to be empty for UI to show the config in General Group.
  private static final ConfigDefinition REQUIRED_FIELDS_CONFIG = new ConfigDefinition(
      ConfigDefinition.REQUIRED_FIELDS, ConfigDef.Type.MODEL, "Required Fields",
      "Records without any of these fields are sent to error",
      null, false, "", null, new ModelDefinition(ModelType.FIELD_SELECTOR_MULTI_VALUED, null, null, null, null),
      "", new ArrayList<>(), 10, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
    Collections.<String>emptyList(), ConfigDef.Evaluation.IMPLICIT, null);

  //Group name needs to be empty for UI to show the config in General Group.
  private static final ConfigDefinition ON_RECORD_ERROR_CONFIG = new ConfigDefinition(
      ConfigDefinition.ON_RECORD_ERROR, ConfigDef.Type.MODEL, "On Record Error",
      "Action to take with records sent to error",
      OnRecordError.TO_ERROR.name(), true, "", null, new ModelDefinition(ModelType.VALUE_CHOOSER,
                                                                         OnRecordErrorChooserValues.class.getName(),
                                                 new OnRecordErrorChooserValues().getValues(),
                                                 new OnRecordErrorChooserValues().getLabels(), null), "",
      new ArrayList<>(), 20, Collections.<ElFunctionDefinition>emptyList(),
      Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
    Collections.<String>emptyList(), ConfigDef.Evaluation.IMPLICIT, null);

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
    //Boolean, Int, long are converted by annotation processor
    //Enum can be left as String
    //Map and classes are converted from json string to map and List.
    if (field.getType().isAssignableFrom(List.class) || field.getType().isAssignableFrom(Map.class)) {
      Object defaultValue = null;
      String defaultValueString = (String) configDef.getDefaultValue();
      if (defaultValueString != null && !defaultValueString.isEmpty()) {
        //the defaultValue string is treated as JSON
        //Because of UI limitations both List and Map must be serialized and de-serialized as List.
        //So Map with 2 entries will be converted 2 Maps.
        // Each Map has 2 entries
        // "key"      ->     <key in the original map>
        //  "value"   ->     <value in the original map>
        defaultValue = ObjectMapperFactory.get().readValue(defaultValueString, List.class);
      }
      configDef.setDefaultValue(defaultValue);
    }
  }

  private void setElMetadata(StageDefinition stageDefinition) throws ClassNotFoundException, IllegalAccessException,
    InstantiationException {
    for(ConfigDefinition configDefinition : stageDefinition.getConfigDefinitions()) {
      updateElFunctionAndConstantDef(stageDefinition, configDefinition);
      if(configDefinition.getModel() != null) {
        List<ConfigDefinition> configDefinitions = configDefinition.getModel().getConfigDefinitions();
        if(configDefinitions != null) {
          for(ConfigDefinition configDef : configDefinitions) {
            updateElFunctionAndConstantDef(stageDefinition, configDef);
          }
        }
      }
    }
  }

  private void updateElFunctionAndConstantDef(StageDefinition stageDefinition, ConfigDefinition configDefinition)
    throws ClassNotFoundException {
    List<Class<?>> classes = new ArrayList<>();
    for(String elDef : configDefinition.getElDefs()) {
      Class<?> elClass = stageDefinition.getStageClassLoader().loadClass(elDef);
      classes.add(elClass);
    }

    //Add the RuntimeEL class for every config property that has text box
    if(configDefinition.getType() != ConfigDef.Type.BOOLEAN && configDefinition.getType() != ConfigDef.Type.MODEL) {
      classes.add(RuntimeEL.class);
    }

    if(!classes.isEmpty()) {
      Class<?>[] elClasses = new Class[classes.size()];
      ELEvaluator elEval = new ELEvaluator(configDefinition.getName(), classes.toArray(elClasses));
      configDefinition.getElFunctionDefinitions().addAll(elEval.getElFunctionDefinitions());
      configDefinition.getElConstantDefinitions().addAll(elEval.getElConstantDefinitions());
    }
  }

}
