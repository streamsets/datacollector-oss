package com.streamsets.pipeline.http;

import com.streamsets.pipeline.log.LogStreamer;
import com.streamsets.pipeline.log.LogUtils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.AuthzRole;
import com.streamsets.pipeline.util.Configuration;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;


@SuppressWarnings("serial")
public class LogServlet extends WebSocketServlet implements WebSocketCreator{
  public static final String X_SDC_LOG_PREVIOUS_OFFSET_HEADER = "X-SDC-LOG-PREVIOUS-OFFSET";

  private final Configuration config;
  private String logFile;

  public LogServlet(Configuration configuration, RuntimeInfo runtimeInfo) {
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
      String offset = request.getParameter("offset");
      if (offset == null) {
        super.service(request, response);
      } else {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/plain");
        try (LogStreamer streamer = new LogStreamer(logFile, Long.parseLong(offset), 20 * 1024)) {
          response.setHeader(X_SDC_LOG_PREVIOUS_OFFSET_HEADER, Long.toString(streamer.getNewEndingOffset()));
          streamer.stream(response.getOutputStream());
        }
      }
    } else {
      response.sendError(HttpServletResponse.SC_FORBIDDEN);
    }
  }
}
