package com.streamsets.pipeline.http;

import com.streamsets.pipeline.log.LogUtils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.AuthzRole;
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


@SuppressWarnings("serial")
public class WebSocketLogServlet extends WebSocketServlet implements WebSocketCreator{

  private final Configuration config;
  private String logFile;

  public WebSocketLogServlet(Configuration configuration, RuntimeInfo runtimeInfo) {
    this.config = configuration;
    try {
      logFile = LogUtils.getLogFile(runtimeInfo);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void configure(WebSocketServletFactory factory) {
    factory.getPolicy().setIdleTimeout(7200000);
    factory.setCreator(this);
  }

  @Override
  public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
    return new LogMessageWebSocket(logFile, config);
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
      IOException {
    if (request.isUserInRole(AuthzRole.ADMIN) ||
        request.isUserInRole(AuthzRole.MANAGER) ||
        request.isUserInRole(AuthzRole.CREATOR)) {
      super.service(request, response);
    } else {
      response.sendError(HttpServletResponse.SC_FORBIDDEN);
    }
  }

}
