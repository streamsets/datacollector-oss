/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.websockets;

import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.prodmanager.StateEventListener;
import com.streamsets.pipeline.util.AuthzRole;
import com.streamsets.pipeline.util.Configuration;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SDCWebSocketServlet extends WebSocketServlet implements WebSocketCreator {
  private final static Logger LOG = LoggerFactory.getLogger(SDCWebSocketServlet.class);

  private final Configuration config;
  private final RuntimeInfo runtimeInfo;
  private final PipelineManager pipelineManager;
  private BlockingQueue<WebSocketMessage> queue;
  private ScheduledExecutorService executorService;

  private static final String MAX_WEB_SOCKETS_CONCURRENT_REQUESTS_KEY = "max.webSockets.concurrent.requests";
  private static final int MAX_WEB_SOCKETS_CONCURRENT_REQUESTS_DEFAULT = 50;
  protected static volatile int webSocketClients;

  public SDCWebSocketServlet(Configuration configuration, RuntimeInfo runtimeInfo,
                             PipelineManager pipelineStateManager) {
    this.config = configuration;
    this.runtimeInfo = runtimeInfo;
    this.pipelineManager = pipelineStateManager;
  }

  @Override
  public void init() throws ServletException {
    super.init();
    queue = new ArrayBlockingQueue<>(10000);
    executorService = new SafeScheduledExecutorService(1, "WebSocket");
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        while (!executorService.isShutdown()) {
          try {
            WebSocketMessage message = queue.poll(100, TimeUnit.MILLISECONDS);
            if (message != null) {
              message.send();
            }
          } catch (InterruptedException ex) {
            //NOP
          } catch (IOException | WebSocketException ex) {
            LOG.warn("Failed to send WebSocket message: {}", ex.getMessage(), ex);
          }
        }
      }
    });
  }

  @Override
  public void destroy() {
    executorService.shutdownNow();
    super.destroy();
  }

  @Override
  public void configure(WebSocketServletFactory factory) {
    factory.getPolicy().setIdleTimeout(7200000);
    factory.setCreator(this);
  }

  @Override
  public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
    HttpServletRequest httpRequest = req.getHttpServletRequest();
    String webSocketType = httpRequest.getParameter("type");
    if(webSocketType != null) {
      switch (webSocketType) {
        case LogMessageWebSocket.TYPE:
          return new LogMessageWebSocket(config, runtimeInfo);
        case StatusWebSocket.TYPE:
          return new StatusWebSocket(new ListenerManager<StateEventListener>() {
            @Override
            public void register(StateEventListener listener) {
              pipelineManager.addStateEventListener(listener);
            }

            @Override
            public void unregister(StateEventListener listener) {
              pipelineManager.removeStateEventListener(listener);
            }
          }, queue);
        case MetricsWebSocket.TYPE:
          return new MetricsWebSocket(new ListenerManager<MetricsEventListener>() {
            @Override
            public void register(MetricsEventListener listener) {
              pipelineManager.addMetricsEventListener(listener);
            }

            @Override
            public void unregister(MetricsEventListener listener) {
              pipelineManager.removeMetricsEventListener(listener);
            }
          }, queue);
        case AlertsWebSocket.TYPE:
          return new AlertsWebSocket(new ListenerManager<AlertEventListener>() {
            @Override
            public void register(AlertEventListener listener) {
              pipelineManager.addAlertEventListener(listener);
            }

            @Override
            public void unregister(AlertEventListener listener) {
              pipelineManager.removeAlertEventListener(listener);
            }
          }, queue);
      }
    }
    return null;
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
      IOException {

    synchronized (SDCWebSocketServlet.class) {
      int maxClients = config.get(MAX_WEB_SOCKETS_CONCURRENT_REQUESTS_KEY, MAX_WEB_SOCKETS_CONCURRENT_REQUESTS_DEFAULT);
      if (webSocketClients < maxClients) {
        webSocketClients++;
      } else {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Maximum WebSockets concurrent connections reached - " +
          webSocketClients);
        return;
      }
    }

    String webSocketType = request.getParameter("type");

    if(webSocketType != null) {
      switch (webSocketType) {
        case LogMessageWebSocket.TYPE:
          if (request.isUserInRole(AuthzRole.ADMIN) ||
            request.isUserInRole(AuthzRole.MANAGER) ||
            request.isUserInRole(AuthzRole.CREATOR)) {
            super.service(request, response);
          } else {
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
          }
          break;

        case StatusWebSocket.TYPE:
        case MetricsWebSocket.TYPE:
        case AlertsWebSocket.TYPE:
          //All roles are supported
          super.service(request, response);
          break;

        default:
          response.sendError(HttpServletResponse.SC_FORBIDDEN);
      }
    }
  }

}
