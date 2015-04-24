/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.domainserver.main;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsModule;
import com.streamsets.pipeline.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

@Module(library = true, injects = {BuildInfo.class, RuntimeInfo.class, Configuration.class},
    includes = MetricsModule.class)
public class RuntimeModule {
  public static final String DOMAIN_SERVER_ID = "sds.id";
  public static final String DOMAIN_SERVER_BASE_HTTP_URL = "sds.base.http.url";
  public static final String SDS_PROPERTY_PREFIX = "sds";

  @Provides
  @Singleton
  public BuildInfo provideBuildInfo() {
    return new BuildInfo();
  }

  @Provides
  @Singleton
  public RuntimeInfo provideRuntimeInfo(MetricRegistry metrics) {
    return new RuntimeInfo(SDS_PROPERTY_PREFIX, metrics, null);
  }

  @Provides
  @Singleton
  public Configuration provideConfiguration(RuntimeInfo runtimeInfo) {
    Configuration.setFileRefsBaseDir(new File(runtimeInfo.getConfigDir()));
    Configuration conf = new Configuration();
    File configFile = new File(runtimeInfo.getConfigDir(), "sds.properties");
    if (configFile.exists()) {
      try {
        conf.load(new FileReader(configFile));
        runtimeInfo.setId(conf.get(DOMAIN_SERVER_ID, runtimeInfo.getId()));
        runtimeInfo.setBaseHttpUrl(conf.get(DOMAIN_SERVER_BASE_HTTP_URL, runtimeInfo.getBaseHttpUrl()));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    return conf;
  }

}
