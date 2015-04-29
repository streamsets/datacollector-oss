/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.websockets;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.util.AuthzRole;
import com.streamsets.pipeline.util.Configuration;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class SDCWebSocketServlet extends WebSocketServlet implements WebSocketCreator {
  private final Configuration config;
  private final RuntimeInfo runtimeInfo;
  private final PipelineManager pipelineManager;


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
          return new StatusWebSocket(pipelineManager);
        case MetricsWebSocket.TYPE:
          return new MetricsWebSocket(pipelineManager);
        case AlertsWebSocket.TYPE:
          return new AlertsWebSocket(pipelineManager);
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
