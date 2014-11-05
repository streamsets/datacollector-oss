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
package com.streamsets.pipeline.stagelibrary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.runner.preview.PlugTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ClassLoaderStageLibrary implements StageLibrary {
  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderStageLibrary.class);

  private static final String PIPELINE_STAGES_JSON = "PipelineStages.json";

  private final List<? extends ClassLoader> stageClassLoaders;
  private Map<String, StageDefinition> stageMap;
  private List<StageDefinition> stageList;
  private final LoadingCache<Locale, List<StageDefinition>> localizedStageList;
  private final ObjectMapper json;

  @Inject
  public ClassLoaderStageLibrary(RuntimeInfo runtimeInfo) {
    stageClassLoaders = runtimeInfo.getStageLibraryClassLoaders();
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
    stageList = new ArrayList<StageDefinition>();
    stageMap = new HashMap<String, StageDefinition>();
    loadStages();
    stageList = ImmutableList.copyOf(stageList);
    stageMap = ImmutableMap.copyOf(stageMap);

    localizedStageList = CacheBuilder.newBuilder().build(new CacheLoader<Locale, List<StageDefinition>>() {
      @Override
      public List<StageDefinition> load(Locale key) throws Exception {
        List<StageDefinition> list = new ArrayList<StageDefinition>();
        for (StageDefinition stage : stageList) {
          list.add(stage.localize(key));
        }
        return list;
      }
    });

  }

  @VisibleForTesting
  void loadStages() {
    if (LOG.isDebugEnabled()) {
      for (ClassLoader cl : stageClassLoaders) {
        LOG.debug("About to load stages from library '{}'", getLibraryName(cl));
      }
    }
    for (ClassLoader cl : stageClassLoaders) {
      String libraryName = getLibraryName(cl);
      LOG.debug("Loading stages from library '{}'", libraryName);
      try {
        Enumeration<URL> resources = null;
        resources = cl.getResources(PIPELINE_STAGES_JSON);
        while (resources.hasMoreElements()) {
          Map<String, String> stagesInLibrary = new HashMap<String, String>();

          URL url = resources.nextElement();
          InputStream is = url.openStream();
          StageDefinition[] stages = json.readValue(is, StageDefinition[].class);
          for (StageDefinition stage : stages) {
            stage.setLibrary(libraryName, cl);
            String key = createKey(libraryName, stage.getName(), stage.getVersion());
            LOG.debug("Loaded stage '{}' (library:name:version)", key);
            if (stagesInLibrary.containsKey(key)) {
              throw new IllegalStateException(String.format(
                  "Library '%s' contains more than one definition for stage '%s', class '%s' and class '%s'",
                  libraryName, key, stagesInLibrary.get(key), stage.getStageClass()));
            }
            stagesInLibrary.put(key, stage.getClassName());
            stageList.add(stage);
            stageMap.put(key, stage);
          }
        }
      } catch (IOException ex) {
        throw new RuntimeException(String.format("Could not load stages definition from '%s', %s", cl, ex.getMessage()),
                                   ex);
      }
    }
  }

  private String createKey(String library, String name, String version) {
    return library + ":" + name + ":" + version;
  }

  @Override
  public List<StageDefinition> getStages() {
    return stageList;
  }

  @Override
  public List<StageDefinition> getStages(Locale locale) {
    try {
      return localizedStageList.get(locale);
    } catch (ExecutionException ex) {
      LOG.warn("Error loading locale '{}', {}", locale, ex.getLocalizedMessage(), ex);
      return stageList;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public StageDefinition getStage(String library, String name, String version) {
    StageDefinition def = null;
    if (library.equals("system")) {
      if (name.equals(":plug:")) {
        def = new StageDefinition(PlugTarget.class.getName(), ":plug:", "1.0.0", "plug", "plug", StageType.TARGET,
                                  Collections.EMPTY_LIST);
        def.setLibrary("system", getClass().getClassLoader());
      }
    } else {
      def = stageMap.get(createKey(library, name, version));
    }
    return def;
  }

  private String getLibraryName(ClassLoader cl) {
    String name;
    try {
      Method method = cl.getClass().getMethod("getName");
      name = (String) method.invoke(cl);
    } catch (NoSuchMethodException ex ) {
      name = "default";
    } catch (Exception ex ) {
      throw new RuntimeException(ex);
    }
    return name;
  }

}
