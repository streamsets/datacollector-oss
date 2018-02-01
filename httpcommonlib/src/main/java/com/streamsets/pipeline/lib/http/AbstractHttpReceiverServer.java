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
package com.streamsets.pipeline.lib.http;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@SuppressWarnings({"squid:S2095", "squid:S00112"})
public abstract class AbstractHttpReceiverServer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHttpReceiverServer.class);

  private final HttpConfigs configs;

  protected final BlockingQueue<Exception> errorQueue;

  private Server httpServer;

  public AbstractHttpReceiverServer(HttpConfigs configs, BlockingQueue<Exception> errorQueue) {
    this.configs = configs;
    this.errorQueue = errorQueue;
  }

  @VisibleForTesting
  int getJettyServerThreads(int maxConcurrentRequests) {
    // per Jetty hardcoded logic, the minimum number of threads we can have is determined by the following formula
    int cores = Runtime.getRuntime().availableProcessors();
    int acceptors = Math.max(1, Math.min(4, cores / 8));
    // In Jetty 9.4, minimum number of threads in Server is updated. -
    // https://github.com/eclipse/jetty.project/commit/ca3af688096687c85ec80e3173380f7d1fe45117
    int selectors = (cores + 1);
    return acceptors + selectors + maxConcurrentRequests;
  }

  @VisibleForTesting
  int getJettyServerMinThreads() {
    return Math.max(configs.getMaxConcurrentRequests() / 2, getJettyServerThreads(1));
  }

  @VisibleForTesting
  int getJettyServerMaxThreads() {
    return getJettyServerThreads(configs.getMaxConcurrentRequests());
  }

  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    int maxThreads = getJettyServerMaxThreads();
    int minThreads = getJettyServerMinThreads();
    QueuedThreadPool threadPool =
        new QueuedThreadPool(maxThreads, minThreads, 60000, new ArrayBlockingQueue<Runnable>(maxThreads));
    threadPool.setName("http-receiver-server:" + context.getPipelineInfo().get(0).getInstanceName());
    threadPool.setDaemon(true);
    Server server = new Server(threadPool);

    ServerConnector connector;
    if (configs.isTlsEnabled()) {
      LOG.debug("Configuring HTTPS");
      HttpConfiguration httpsConf = new HttpConfiguration();
      httpsConf.addCustomizer(new SecureRequestCustomizer());
      SslContextFactory sslContextFactory = new SslContextFactory();

      TlsConfigBean tlsConfig = configs.getTlsConfigBean();
      try {
        sslContextFactory.setKeyStorePath(tlsConfig.keyStoreFilePath);
        sslContextFactory.setKeyStoreType(tlsConfig.keyStoreType.getJavaValue());
        sslContextFactory.setKeyStorePassword(tlsConfig.keyStorePassword.get());
        sslContextFactory.setKeyManagerPassword(tlsConfig.keyStorePassword.get());
        sslContextFactory.setIncludeProtocols(tlsConfig.getFinalProtocols());
        sslContextFactory.setIncludeCipherSuites(tlsConfig.getFinalCipherSuites());
      } catch (Exception e) {
        issues.add(context.createConfigIssue("HTTP", "", HttpServerErrors.HTTP_SERVER_ORIG_12, e.getMessage()));
      }
      connector = new ServerConnector(server,
          new SslConnectionFactory(sslContextFactory, "http/1.1"),
          new HttpConnectionFactory(httpsConf)
      );
    } else {
      LOG.debug("Configuring HTTP");
      connector = new ServerConnector(server);
    }
    connector.setPort(configs.getPort());
    server.setConnectors(new Connector[]{connector});

    ServletContextHandler contextHandler = new ServletContextHandler();
    // CORS Handling
    FilterHolder crossOriginFilter = new FilterHolder(CrossOriginFilter.class);
    Map<String, String> params = new HashMap<>();
    params.put(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
    params.put(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "*");
    crossOriginFilter.setInitParameters(params);
    contextHandler.addFilter(crossOriginFilter, "/*", EnumSet.of(DispatcherType.REQUEST));

    addReceiverServlet(context, contextHandler);

    contextHandler.setContextPath("/");
    server.setHandler(contextHandler);

    httpServer = server;

    return issues;
  }

  public void startServer()  throws StageException {
    try {
      httpServer.start();
      LOG.debug("Running, port '{}', TLS '{}'", configs.getPort(), configs.isTlsEnabled());
    } catch (Exception e) {
       throw new StageException(HttpServerErrors.HTTP_SERVER_ORIG_20, e.getMessage());
    }
  }

  public boolean isRunning()  throws StageException {
    return httpServer.isRunning();
  }

  public void destroy() {
    LOG.debug("Shutting down, port '{}', TLS '{}'", configs.getPort(), configs.isTlsEnabled());
    if (httpServer != null) {
      try {
        setShuttingDown();
        httpServer.stop();
      } catch (Exception ex) {
        LOG.warn("Error while shutting down: {}", ex.toString(), ex);
      }
      httpServer = null;
    }
  }

  public abstract void addReceiverServlet(Stage.Context context, ServletContextHandler contextHandler);

  public abstract void setShuttingDown();

}
