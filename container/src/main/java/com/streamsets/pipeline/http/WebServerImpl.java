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
import com.google.common.base.Preconditions;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import javax.inject.Inject;
import java.util.Set;

public class WebServerImpl implements WebServer {
  private MetricRegistry metrics;
  private Set<ContextConfigurator> contextConfigurators;
  private JmxReporter reporter;
  private Server server;
  private volatile boolean started;
  private volatile boolean destroyed;

  @Inject
  public WebServerImpl(MetricRegistry metrics, Set<ContextConfigurator> contextConfigurators) {
    this.metrics = metrics;
    this.contextConfigurators = contextConfigurators;
  }

  public void init() {
    server = new Server(8080);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    for (ContextConfigurator cc : contextConfigurators) {
      cc.configure(context);
    }
    server.setHandler(context);
  }

  @Override
  public synchronized void start() {
    Preconditions.checkState(!started, "Already started");
    Preconditions.checkState(!destroyed, "Already destroyed");
    started = true;
    reporter = JmxReporter.forRegistry(metrics).build();
    reporter.start();
    try {
      server.start();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public synchronized void stop() {
    Preconditions.checkState(started, "Not started");
    if (!destroyed) {
      destroyed = true;
      try {
        server.stop();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      } finally {
        reporter.stop();
        reporter.close();
      }
    }
  }
}
