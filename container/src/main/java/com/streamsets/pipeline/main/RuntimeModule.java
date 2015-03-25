/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Module(library = true, injects = {BuildInfo.class, RuntimeInfo.class, Configuration.class})
public class RuntimeModule {
  public static final String DATA_COLLECTOR_ID = "sdc.id";
  public static final String DATA_COLLECTOR_BASE_HTTP_URL = "sdc.base.http.url";
  public static final String STAGE_MEMORY_LIMIT_PREFIX = "production.stage.memoryLimit.";

  private static List<ClassLoader> stageLibraryClassLoaders = ImmutableList.of(RuntimeModule.class.getClassLoader());

  public static synchronized void setStageLibraryClassLoaders(List<? extends ClassLoader> classLoaders) {
    stageLibraryClassLoaders = ImmutableList.copyOf(classLoaders);
  }

  @Provides @Singleton
  public BuildInfo provideBuildInfo() {
    return new BuildInfo();
  }

  @Provides @Singleton
  public RuntimeInfo provideRuntimeInfo() {
    return new RuntimeInfo(stageLibraryClassLoaders);
  }

  @Provides @Singleton
  public Configuration provideConfiguration(RuntimeInfo runtimeInfo) {
    Configuration.setFileRefsBaseDir(new File(runtimeInfo.getConfigDir()));
    Configuration conf = new Configuration();
    File configFile = new File(runtimeInfo.getConfigDir(), "sdc.properties");
    if (configFile.exists()) {
      try {
        conf.load(new FileReader(configFile));
        runtimeInfo.setId(conf.get(DATA_COLLECTOR_ID, runtimeInfo.getId()));
        Map<String, String> memoryLimitConfigs = new HashMap<>();
        for (String key : conf.getNames()) {
          if (key.startsWith(STAGE_MEMORY_LIMIT_PREFIX)) {
            memoryLimitConfigs.put(key.substring(STAGE_MEMORY_LIMIT_PREFIX.length()), conf.get(key, null));
          }
        }
        runtimeInfo.setMemoryLimitConfiguration(new MemoryLimitConfiguration(memoryLimitConfigs));
        runtimeInfo.setBaseHttpUrl(conf.get(DATA_COLLECTOR_BASE_HTTP_URL, runtimeInfo.getBaseHttpUrl()));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    return conf;
  }

}
