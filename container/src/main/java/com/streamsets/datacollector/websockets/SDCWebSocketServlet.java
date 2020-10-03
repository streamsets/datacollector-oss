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
package com.streamsets.datacollector.websockets;

import com.streamsets.datacollector.alerts.AlertEventListener;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsEventListener;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
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
  private final EventListenerManager eventListenerManager;
  private BlockingQueue<WebSocketMessage> queue;
  private ScheduledExecutorService executorService;

  public static final String MAX_WEB_SOCKETS_CONCURRENT_REQUESTS_KEY = "max.webSockets.concurrent.requests";
  public static final int MAX_WEB_SOCKETS_CONCURRENT_REQUESTS_DEFAULT = 50;
  protected static volatile int webSocketClients;

  public SDCWebSocketServlet(Configuration configuration, RuntimeInfo runtimeInfo,
                             EventListenerManager eventListenerManager) {
    this.config = configuration;
    this.runtimeInfo = runtimeInfo;
    this.eventListenerManager = eventListenerManager;
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
            LOG.warn("Failed to send WebSocket message: {}", ex.toString(), ex);
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
    final String pipelineName = httpRequest.getParameter("pipelineName");
    if(webSocketType != null) {
      switch (webSocketType) {
        case LogMessageWebSocket.TYPE:
          return new LogMessageWebSocket(config, runtimeInfo);
        case StatusWebSocket.TYPE:
          return new StatusWebSocket(new ListenerManager<StateEventListener>() {
            @Override
            public void register(StateEventListener listener) {
              eventListenerManager.addStateEventListener(listener);
            }

            @Override
            public void unregister(StateEventListener listener) {
              eventListenerManager.removeStateEventListener(listener);
            }
          }, queue);
        case MetricsWebSocket.TYPE:
          return new MetricsWebSocket(new ListenerManager<MetricsEventListener>() {
            @Override
            public void register(MetricsEventListener listener) {
              eventListenerManager.addMetricsEventListener(pipelineName, listener);
            }

            @Override
            public void unregister(MetricsEventListener listener) {
              eventListenerManager.removeMetricsEventListener(pipelineName, listener);
            }
          }, queue);
        case AlertsWebSocket.TYPE:
          return new AlertsWebSocket(new ListenerManager<AlertEventListener>() {
            @Override
            public void register(AlertEventListener listener) {
              eventListenerManager.addAlertEventListener(listener);
            }

            @Override
            public void unregister(AlertEventListener listener) {
              eventListenerManager.removeAlertEventListener(listener);
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
            request.isUserInRole(AuthzRole.CREATOR) ||
            request.isUserInRole(AuthzRole.ADMIN_REMOTE) ||
            request.isUserInRole(AuthzRole.MANAGER_REMOTE) ||
            request.isUserInRole(AuthzRole.CREATOR_REMOTE)) {
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
