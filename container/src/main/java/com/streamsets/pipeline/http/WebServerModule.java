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
import com.streamsets.pipeline.container.ContainerModule;
import com.streamsets.pipeline.container.PipelineRunner;
import com.streamsets.pipeline.metrics.MetricsModule;
import com.streamsets.pipeline.restapi.PipelineRestAPI;
import com.streamsets.pipeline.restapi.RestAPI;
import dagger.Module;
import dagger.Provides;
import dagger.Provides.Type;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

@Module(library = true, includes = {MetricsModule.class, ContainerModule.class})
public class WebServerModule {

  @Provides
  WebServer provideWebServer(WebServerImpl webServer) {
    return webServer;
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
  ContextConfigurator provideJersey(final MetricRegistry metrics) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute("com.codahale.metrics.servlets.MetricsServlet.registry", metrics);
        ServletHolder servlet = new ServletHolder(new ServletContainer());
        servlet.setInitParameter("jersey.config.server.provider.packages", RestAPI.class.getPackage().getName());
        context.addServlet(servlet, "/rest/*");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineRunnerForPreview(final PipelineRunner pipelineRunner) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(PipelineRestAPI.PIPELINE, pipelineRunner);
      }
    };
  }

}
