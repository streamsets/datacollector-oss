/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.http;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestWebServerTask {


  @SuppressWarnings("unchecked")
  private WebServerTask createWebServerTask(
      final String confDir, final Configuration conf, final Set<WebAppProvider> webAppProviders
  ) throws Exception {
    RuntimeInfo runtimeInfo =
        new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(), Collections
            .<ClassLoader>emptyList()) {
          @Override
          public String getConfigDir() {
            return confDir;
          }
        };
    Set<ContextConfigurator> configurators = new HashSet<>();
    return new WebServerTask(runtimeInfo, conf, configurators, webAppProviders) {
      @Override
      protected String getAppAuthToken(Configuration appConfiguration) {
        return "applicationToken";
      }

      @Override
      protected String getComponentId(Configuration appConfiguration) {
        return "componentId";
      }
    };
  }

  @Test
  public void testConfigurationInitialization() throws Exception {
    Configuration serverConf = new Configuration();
    final Configuration appConf = new Configuration();

    WebAppProvider webAppProvider = new WebAppProvider() {
      @Override
      public Configuration getAppConfiguration() {
        return appConf;
      }

      @Override
      public ServletContextHandler get() {
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/foo");
        return handler;
      }

      @Override
      public void postStart() {

      }
    };


    WebServerTask webServerTask =
        createWebServerTask(new File("target").getAbsolutePath(), serverConf, ImmutableSet.of(webAppProvider));
    webServerTask = Mockito.spy(webServerTask);

    try {
      webServerTask.initTask();
      Mockito.verify(webServerTask, Mockito.times(1)).createSecurityHandler(Mockito.<Server>any(), Mockito.eq
              (serverConf),
          Mockito.<ServletContextHandler>any(),Mockito.eq("/"));
      Mockito.verify(webServerTask, Mockito.times(1)).createSecurityHandler(Mockito.<Server>any(), Mockito.eq
              (appConf),
          Mockito.<ServletContextHandler>any(),Mockito.eq("/foo"));
    } finally {
      webServerTask.stopTask();
    }
  }

  @Test
  public void testInjectionOfComponentIdAndAppToken() throws Exception {
    Configuration serverConf = new Configuration();
    serverConf.set(WebServerTask.DPM_ENABLED, true);
    serverConf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "applicationToken");

    WebServerTask webServerTask =
        createWebServerTask(new File("target").getAbsolutePath(), serverConf, Collections.<WebAppProvider>emptySet());
    webServerTask = Mockito.spy(webServerTask);

    RemoteSSOService ssoService = Mockito.mock(RemoteSSOService.class);
    Mockito.doReturn(ssoService).when(webServerTask).createRemoteSSOService(Mockito.<Configuration>any());

    try {
      webServerTask.initTask();
      Mockito.verify(ssoService, Mockito.times(1)).setApplicationAuthToken(Mockito.eq("applicationToken"));
      Mockito.verify(ssoService, Mockito.times(1)).setComponentId(Mockito.eq("componentId"));
    } finally {
      webServerTask.stopTask();
    }
  }

}
