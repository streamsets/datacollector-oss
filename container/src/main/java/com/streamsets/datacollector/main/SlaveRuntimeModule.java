/*
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
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.runner.common.Constants;
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

@Module(library = true, injects = {BuildInfo.class, RuntimeInfo.class, Configuration.class, EventListenerManager
    .class}, includes = MetricsModule.class)
public class SlaveRuntimeModule {
  private static final Logger LOG = LoggerFactory.getLogger(SlaveRuntimeModule.class);
  public static final String SDC_PROPERTY_PREFIX = "sdc";
  public static final String PIPELINE_ACCESS_CONTROL_ENABLED = "pipeline.access.control.enabled";
  public static final boolean PIPELINE_ACCESS_CONTROL_ENABLED_DEFAULT = false;
  private static List<ClassLoader> stageLibraryClassLoaders = Collections.emptyList();

  // Called by BootstrapCluster through reflection
  public static synchronized void setStageLibraryClassLoaders(List<? extends ClassLoader> classLoaders) {
    stageLibraryClassLoaders = ImmutableList.copyOf(classLoaders);
  }

  @Provides
  @Singleton
  public BuildInfo provideBuildInfo() {
    return new DataCollectorBuildInfo();
  }

  @Provides
  @Singleton
  public RuntimeInfo provideRuntimeInfo(MetricRegistry metrics) {
    RuntimeInfo info = new SlaveRuntimeInfo(SDC_PROPERTY_PREFIX, metrics, stageLibraryClassLoaders);
    info.init();
    return info;
  }

  @Provides
  @Singleton
  public Configuration provideConfiguration(RuntimeInfo runtimeInfo) {
    Configuration.setFileRefsBaseDir(new File(runtimeInfo.getConfigDir()));
    Configuration conf = new Configuration();
    File configFile = new File(runtimeInfo.getConfigDir(), "sdc.properties");
    if (configFile.exists()) {
      try (FileReader reader = new FileReader(configFile)) {
        conf.load(reader);
        ((SlaveRuntimeInfo) runtimeInfo).setMasterSDCId(Utils.checkNotNull(conf.get(Constants.SDC_ID, null),
            Constants.SDC_ID
        ));
        String remote = Utils.checkNotNull(conf.get(ClusterModeConstants.CLUSTER_PIPELINE_REMOTE, null),
            ClusterModeConstants.CLUSTER_PIPELINE_REMOTE
        );
        ((SlaveRuntimeInfo) runtimeInfo).setRemotePipeline(Boolean.valueOf(remote));
        String appAuthToken = conf.get(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "").trim();
        runtimeInfo.setAppAuthToken(appAuthToken);
        boolean isDPMEnabled = conf.get(RemoteSSOService.DPM_ENABLED, RemoteSSOService.DPM_ENABLED_DEFAULT);
        runtimeInfo.setDPMEnabled(isDPMEnabled);
        String deploymentId = conf.get(RemoteSSOService.DPM_DEPLOYMENT_ID, null);
        runtimeInfo.setDeploymentId(deploymentId);
        boolean aclEnabled = conf.get(PIPELINE_ACCESS_CONTROL_ENABLED, PIPELINE_ACCESS_CONTROL_ENABLED_DEFAULT);
        runtimeInfo.setAclEnabled(aclEnabled);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    } else {
      LOG.error("Error did not find sdc.properties at expected location: {}", configFile);
    }
    return conf;
  }

  @Provides
  @Singleton
  public EventListenerManager provideEventListenerManager() {
    return new EventListenerManager();
  }

  @Provides @Singleton
  public UserGroupManager provideUserGroupManager() {
    return new FileUserGroupManager();
  }

}
