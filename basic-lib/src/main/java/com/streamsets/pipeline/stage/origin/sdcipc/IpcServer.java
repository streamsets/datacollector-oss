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
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.stage.destination.sdcipc.Constants;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"squid:S2095", "squid:S00112"})
public class IpcServer {
  private static final Logger LOG = LoggerFactory.getLogger(IpcServer.class);

  private final Stage.Context context;
  private final Configs configs;
  private Server httpServer;
  private final BlockingQueue<List<Record>> queue;
  private IpcServlet servlet;

  public IpcServer(Stage.Context context, Configs configs) {
    this.context = context;
    this.configs = configs;
    queue = new SynchronousQueue<>();
  }

  private int getJettyServerMinimumThreads() {
    // per Jetty hardcoded logic, them minimum number of threads we can have is determined by the following formula
    int cores = Runtime.getRuntime().availableProcessors();
    int acceptors = Math.max(1, Math.min(4,cores/8));
    // In Jetty 9.4, minimum number of threads in Server is updated. -
    // https://github.com/eclipse/jetty.project/commit/ca3af688096687c85ec80e3173380f7d1fe45117
    int selectors = (cores + 1);
    return acceptors + selectors + 1;
  }

  public void start() throws Exception {
    int numberOfThreads = getJettyServerMinimumThreads();
    QueuedThreadPool threadPool = new QueuedThreadPool(numberOfThreads, numberOfThreads, 60000,
                                                       new ArrayBlockingQueue<Runnable>(20));
    threadPool.setName("sdcipc-server");
    threadPool.setDaemon(true);
    Server server = new Server(threadPool);

    ServerConnector connector;
    if (configs.tlsConfigBean.isEnabled()) {
      LOG.debug("Configuring over HTTPS");
      HttpConfiguration httpsConf = new HttpConfiguration();
      httpsConf.addCustomizer(new SecureRequestCustomizer());
      SslContextFactory sslContextFactory = new SslContextFactory();
      final TlsConfigBean tlsConfig = configs.getTlsConfigBean();

      sslContextFactory.setKeyStore(tlsConfig.getKeyStore());
      sslContextFactory.setKeyStorePassword(tlsConfig.keyStorePassword.get());
      sslContextFactory.setKeyManagerPassword(tlsConfig.keyStorePassword.get());
      sslContextFactory.setSslContext(tlsConfig.getSslContext());
      sslContextFactory.setIncludeProtocols(tlsConfig.getFinalProtocols());
      sslContextFactory.setIncludeCipherSuites(tlsConfig.getFinalCipherSuites());

      connector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory, "http/1.1"),
                                      new HttpConnectionFactory(httpsConf));
    } else {
      LOG.debug("Configuring over HTTP");
      connector = new ServerConnector(server);
    }
    connector.setPort(configs.port);
    server.setConnectors(new Connector[]{connector});

    servlet = new IpcServlet(context, configs, queue);
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.addFilter(DisableTraceFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
    contextHandler.addServlet(new ServletHolder(new PingServlet()), Constants.PING_PATH);
    contextHandler.addServlet(new ServletHolder(servlet), Constants.IPC_PATH);
    contextHandler.setContextPath("/");
    server.setHandler(contextHandler);

    server.start();

    LOG.info("Running, port '{}', TLS '{}'", configs.port, configs.tlsConfigBean.isEnabled());

    httpServer = server;
  }

  public static class DisableTraceFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
      // Empty
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
      HttpServletRequest httpRequest = (HttpServletRequest)request;
      HttpServletResponse httpResponse = (HttpServletResponse) response;
      if("TRACE".equals(httpRequest.getMethod())) {
        httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
      } else {
        filterChain.doFilter(request, response);
      }
    }

    @Override
    public void destroy() {
      // Empty
    }
  }

  public void stop() {
    LOG.info("Shutting down, port '{}', TLS '{}'", configs.port, configs.tlsConfigBean.isEnabled());
    if (httpServer != null) {
      try {
        servlet.setShuttingDown();
        try {
          // wait up to 30secs for servlet to finish POST request then continue with the shutdown
          long start = System.currentTimeMillis();
          while (servlet.isInPost() && System.currentTimeMillis() - start < 30000) {
            Thread.sleep(50);
          }
          if (servlet.isInPost()) {
            // release the queue before forcing a shutdown
            cancelBatch();
            LOG.warn("Servlet not completing POST after 30secs, forcing a shutdown");
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }

        httpServer.stop();
      } catch (Exception ex) {
        LOG.warn("Error while shutting down: {}", ex.toString(), ex);
      }
      httpServer = null;
    }
  }

  public List<Record> poll(long secs) throws InterruptedException {
    return queue.poll(secs, TimeUnit.SECONDS);
  }

  public void cancelBatch() {
    LOG.debug("Cancel batch");
    servlet.batchCancelled();
    synchronized (queue) {
      queue.notify();
    }
  }

  public void doneWithBatch() {
    LOG.debug("Done with batch");
    servlet.batchDone();
    synchronized (queue) {
      queue.notify();
    }
  }

}
