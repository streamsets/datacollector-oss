/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Set;

public class WebServerTask extends AbstractTask {
  private static final String PORT_NUMBER_KEY = "http.port";
  private static final int PORT_NUMBER_DEFAULT = 8080;

  private static final Logger LOG = LoggerFactory.getLogger(WebServerTask.class);

  private final Configuration conf;
  private final Set<ContextConfigurator> contextConfigurators;
  private int port;
  private Server server;

  @Inject
  public WebServerTask(Configuration conf, Set<ContextConfigurator> contextConfigurators) {
    super("webServer");
    this.conf = conf;
    this.contextConfigurators = contextConfigurators;
  }

  @Override
  protected void initTask() {
    port = conf.get(PORT_NUMBER_KEY, PORT_NUMBER_DEFAULT);
    server = new Server(port);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    for (ContextConfigurator cc : contextConfigurators) {
      cc.init(context);
    }
    server.setHandler(context);
  }

  @Override
  protected void runTask() {
    for (ContextConfigurator cc : contextConfigurators) {
      cc.start();
    }
    try {
      server.start();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    LOG.debug("Running on port '{}'", port);
  }

  @Override
  protected void stopTask() {
    try {
      server.stop();
    } catch (Exception ex) {
      LOG.error("Error while stopping Jetty, {}", ex.getMessage(), ex);
    } finally {
      for (ContextConfigurator cc : contextConfigurators) {
        try {
          cc.stop();
        } catch (Exception ex) {
          LOG.error("Error while stopping '{}', {}", cc.getClass().getSimpleName(), ex.getMessage(), ex);
        }
      }
    }
  }
}
