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
package com.streamsets.pipeline.http;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.agent.RuntimeModule;
import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.container.Configuration;
import com.streamsets.pipeline.metrics.MetricsModule;
import com.streamsets.pipeline.restapi.RestAPI;
import com.streamsets.pipeline.restapi.configuration.ConfigurationInjector;
import com.streamsets.pipeline.restapi.configuration.PipelineStoreInjector;
import com.streamsets.pipeline.restapi.configuration.RestAPIResourceConfig;
import com.streamsets.pipeline.restapi.configuration.StageLibraryInjector;
import com.streamsets.pipeline.stagelibrary.StageLibrary;
import com.streamsets.pipeline.stagelibrary.StageLibraryModule;
import com.streamsets.pipeline.store.PipelineStore;
import com.streamsets.pipeline.store.PipelineStoreModule;
import dagger.Module;
import dagger.Provides;
import dagger.Provides.Type;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;

@Module(library = true, includes = {RuntimeModule.class, MetricsModule.class,
    PipelineStoreModule.class, StageLibraryModule.class})
public class WebServerModule {

  @Provides
  WebServer provideWebServer(WebServerImpl webServer) {
    return webServer;
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideStaticWeb(final RuntimeInfo runtimeInfo) {
    return new ContextConfigurator() {

      @Override
      public void init(ServletContextHandler context) {
        ServletHolder servlet = new ServletHolder(new DefaultServlet());
        servlet.setInitParameter("dirAllowed", "true");
        servlet.setInitParameter("resourceBase", runtimeInfo.getStaticWebDir());
        context.addServlet(servlet, "/*");
      }

    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideJMX(final MetricRegistry metrics) {
    return new ContextConfigurator() {
      private JmxReporter reporter;
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute("com.codahale.metrics.servlets.MetricsServlet.registry", metrics);
        ServletHolder servlet = new ServletHolder(new JMXJsonServlet());
        context.addServlet(servlet, "/jmx");
      }

      @Override
      public void start() {
        reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
      }

      @Override
      public void stop() {
        reporter.stop();
        reporter.close();
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideJersey() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        ServletHolder servlet = new ServletHolder(new ServletContainer());
        servlet.setInitParameter(ServerProperties.PROVIDER_PACKAGES, RestAPI.class.getPackage().getName());
        servlet.setInitParameter(ServletProperties.JAXRS_APPLICATION_CLASS, RestAPIResourceConfig.class.getName());
        context.addServlet(servlet, "/rest/*");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final PipelineStore pipelineStore) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(PipelineStoreInjector.PIPELINE_STORE, pipelineStore);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final StageLibrary stageLibrary) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(StageLibraryInjector.STAGE_LIBRARY, stageLibrary);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final Configuration configuration) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(ConfigurationInjector.CONFIGURATION, configuration);
      }
    };
  }

}
