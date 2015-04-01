/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.metrics.MetricsModule;
import com.streamsets.pipeline.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

@Module(library = true, injects = {BuildInfo.class, RuntimeInfo.class, Configuration.class},
    includes = MetricsModule.class)
public class RuntimeModule {
  public static final String DATA_COLLECTOR_ID = "sdc.id";
  public static final String DATA_COLLECTOR_BASE_HTTP_URL = "sdc.base.http.url";

  private static List<ClassLoader> stageLibraryClassLoaders = ImmutableList.of(RuntimeModule.class.getClassLoader());

  public static synchronized void setStageLibraryClassLoaders(List<? extends ClassLoader> classLoaders) {
    stageLibraryClassLoaders = ImmutableList.copyOf(classLoaders);
  }

  @Provides @Singleton
  public BuildInfo provideBuildInfo() {
    return new BuildInfo();
  }

  @Provides @Singleton
  public RuntimeInfo provideRuntimeInfo(MetricRegistry metrics) {
    return new RuntimeInfo(metrics, stageLibraryClassLoaders);
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
        runtimeInfo.setBaseHttpUrl(conf.get(DATA_COLLECTOR_BASE_HTTP_URL, runtimeInfo.getBaseHttpUrl()));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    return conf;
  }

}
