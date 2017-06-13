/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.main;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.metrics.MetricsModule;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.pipeline.api.impl.Utils;
import dagger.Module;
import dagger.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Module(library = true, injects = {
    BuildInfo.class,
    RuntimeInfo.class,
    Configuration.class,
    EventListenerManager.class,
    UserGroupManager.class
}, includes = MetricsModule.class)
public class RuntimeModule {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeModule.class);
  public static final String DATA_COLLECTOR_BASE_HTTP_URL = "sdc.base.http.url";
  public static final String SDC_PROPERTY_PREFIX = "sdc";
  public static final String PIPELINE_EXECUTION_MODE_KEY = "pipeline.execution.mode";
  public static final String PIPELINE_ACCESS_CONTROL_ENABLED = "pipeline.access.control.enabled";
  public static final boolean PIPELINE_ACCESS_CONTROL_ENABLED_DEFAULT = false;
  private static List<ClassLoader> stageLibraryClassLoaders = Collections.emptyList();//ImmutableList.of(RuntimeModule.class.getClassLoader());

  public static synchronized void setStageLibraryClassLoaders(List<? extends ClassLoader> classLoaders) {
    stageLibraryClassLoaders = ImmutableList.copyOf(classLoaders);
  }

  @Provides @Singleton
  public BuildInfo provideBuildInfo() {
    return new DataCollectorBuildInfo();
  }

  @Provides @Singleton
  public RuntimeInfo provideRuntimeInfo(MetricRegistry metrics) {
    RuntimeInfo info = new StandaloneRuntimeInfo(SDC_PROPERTY_PREFIX, metrics, stageLibraryClassLoaders);
    info.init();
    return info;
  }

  @Provides @Singleton
  public Configuration provideConfiguration(RuntimeInfo runtimeInfo) {
    Configuration.setFileRefsBaseDir(new File(runtimeInfo.getConfigDir()));
    Configuration conf = new Configuration();
    File configFile = new File(runtimeInfo.getConfigDir(), "sdc.properties");
    if (configFile.exists()) {
      try(FileReader reader = new FileReader(configFile)) {
        conf.load(reader);
        runtimeInfo.setBaseHttpUrl(conf.get(DATA_COLLECTOR_BASE_HTTP_URL, runtimeInfo.getBaseHttpUrl()));
        String appAuthToken = conf.get(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "").trim();
        runtimeInfo.setAppAuthToken(appAuthToken);
        boolean isDPMEnabled = conf.get(RemoteSSOService.DPM_ENABLED, RemoteSSOService.DPM_ENABLED_DEFAULT);
        runtimeInfo.setDPMEnabled(isDPMEnabled);
        boolean aclEnabled = conf.get(PIPELINE_ACCESS_CONTROL_ENABLED, PIPELINE_ACCESS_CONTROL_ENABLED_DEFAULT);
        String auth = conf.get(WebServerTask.AUTHENTICATION_KEY, WebServerTask.AUTHENTICATION_DEFAULT);
        if (aclEnabled && (!"none".equals(auth) || isDPMEnabled)) {
          runtimeInfo.setAclEnabled(true);
        } else {
          runtimeInfo.setAclEnabled(false);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    } else {
      LOG.error("Error did not find sdc.properties at expected location: {}", configFile);
    }
    return conf;
  }

  @Provides @Singleton
  public EventListenerManager provideEventListenerManager() {
    return new EventListenerManager();
  }

  @Provides @Singleton
  public UserGroupManager provideUserGroupManager(Configuration configuration) {
    String loginModule = configuration.get(
        WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE,
        WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE_DEFAULT
    );
    switch (loginModule) {
      case WebServerTask.FILE:
        return new FileUserGroupManager();
      case WebServerTask.LDAP:
        return new LdapUserGroupManager();
      default:
        throw new RuntimeException(Utils.format("Invalid Authentication Login Module '{}', must be one of '{}'",
            loginModule, WebServerTask.LOGIN_MODULES));
    }
  }

}
