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

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.util.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Set;

public class WebServerImpl implements WebServer {
  private static final String PORT_NUMBER_KEY = "http.port";
  private static final int PORT_NUMBER_DEFAULT = 8080;

  private static final Logger LOG = LoggerFactory.getLogger(WebServerImpl.class);

  private final Configuration conf;
  private final Set<ContextConfigurator> contextConfigurators;
  private int port;
  private Server server;
  private volatile boolean started;
  private volatile boolean destroyed;

  @Inject
  public WebServerImpl(Configuration conf, Set<ContextConfigurator> contextConfigurators) {
    this.conf = conf;
    this.contextConfigurators = contextConfigurators;
  }

  public void init() {
    LOG.debug("Initializing");
    port = conf.get(PORT_NUMBER_KEY, PORT_NUMBER_DEFAULT);
    server = new Server(port);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    for (ContextConfigurator cc : contextConfigurators) {
      cc.init(context);
    }
    server.setHandler(context);
    LOG.debug("Initialized");
  }

  @Override
  public synchronized void start() {
    LOG.debug("Starting");
    Preconditions.checkState(!started, "Already started");
    Preconditions.checkState(!destroyed, "Already destroyed");
    started = true;
    for (ContextConfigurator cc : contextConfigurators) {
      cc.start();
    }
    try {
      server.start();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    LOG.debug("Started on port '{}'", port);
  }

  @Override
  public synchronized void stop() {
    Preconditions.checkState(started, "Not started");
    if (!destroyed) {
      LOG.debug("Stopping");
      destroyed = true;
      try {
        server.stop();
      } catch (Exception ex) {
        LOG.error("Stopping Jetty: {}", ex.getMessage(), ex);
      } finally {
        for (ContextConfigurator cc : contextConfigurators) {
          try {
            cc.stop();
          } catch (Exception ex) {
            LOG.error("Stopping ContextConfigurator: {}", ex.getMessage(), ex);
          }
        }
      }
      LOG.debug("Stopped");
    }
  }
}
