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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ErrorHandlingChooserValues;
import com.streamsets.pipeline.config.PipelineDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageLibraryDefinition;
import com.streamsets.pipeline.definition.StageDefinitionExtractor;
import com.streamsets.pipeline.definition.StageLibraryDefinitionExtractor;
import com.streamsets.pipeline.el.RuntimeEL;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.task.AbstractTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ClassLoaderStageLibraryTask extends AbstractTask implements StageLibraryTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderStageLibraryTask.class);

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
  public void initTask() {
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
      int libs = 0;
      int stages = 0;
      long start = System.currentTimeMillis();
      LocaleInContext.set(Locale.getDefault());
      for (ClassLoader cl : stageClassLoaders) {
        libs++;
        StageLibraryDefinition libDef = StageLibraryDefinitionExtractor.get().extract(cl);
        LOG.debug("Loading stages from library '{}'", libDef.getName());
        try {
          Enumeration<URL> resources = cl.getResources(STAGES_DEFINITION_RESOURCE);
          while (resources.hasMoreElements()) {
            Map<String, String> stagesInLibrary = new HashMap<>();
            URL url = resources.nextElement();
            try (InputStream is = url.openStream()) {
              Map<String, List<String>> libraryInfo = json.readValue(is, Map.class);
              for (String className : libraryInfo.get("stageClasses")) {
                stages++;
                Class<? extends Stage> klass = (Class<? extends Stage>) cl.loadClass(className);
                StageDefinition stage = StageDefinitionExtractor.get().
                    extract(libDef, klass, Utils.formatL("Library='{}'", libDef.getName()));
                String key = createKey(libDef.getName(), stage.getName(), stage.getVersion());
                LOG.debug("Loaded stage '{}' (library:name:version)", key);
                if (stagesInLibrary.containsKey(key)) {
                  throw new IllegalStateException(Utils.format(
                      "Library '{}' contains more than one definition for stage '{}', class '{}' and class '{}'",
                      libDef.getName(), key, stagesInLibrary.get(key), stage.getStageClass()));
                }
                stagesInLibrary.put(key, stage.getClassName());
                stageList.add(stage);
                stageMap.put(key, stage);
                computeDependsOnChain(stage);
              }
            }
          }
        } catch (IOException | ClassNotFoundException ex) {
          throw new RuntimeException(
              Utils.format("Could not load stages definition from '{}', {}", cl, ex.getMessage()), ex);
        }
      }
      LOG.debug("Loaded '{}' libraries with a total of '{}' stages in '{}ms'", libs, stages,
                System.currentTimeMillis() - start);
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

  private String createKey(String library, String name, String version) {
    return library + ":" + name + ":" + version;
  }

  @Override
  public PipelineDefinition getPipeline() {
    return PipelineDefinition.getPipelineDef();
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

  @Override
  public ClassLoader getStageClassLoader(StageDefinition stageDefinition) {
    return stageDefinition.getStageClassLoader(); //TODO get private classloader if necessary
  }

  @Override
  public void releaseStageClassLoader(ClassLoader classLoader) {
    //TODO release if private classloader
  }

}
