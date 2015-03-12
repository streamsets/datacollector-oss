package com.streamsets.pipeline.http;

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
  private final static Logger LOG = LoggerFactory.getLogger(LogServlet.class);
  static final String LOG4J_APPENDER_STREAMSETS_FILE = "log4j.appender.streamsets.File";


  private final Configuration config;
  private String logFile;

  public LogServlet(Configuration configuration, RuntimeInfo runtimeInfo) {
    this.config = configuration;
    URL log4jConfig = runtimeInfo.getAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR);
    if (log4jConfig != null) {
      try (InputStream is = log4jConfig.openStream()) {
        Properties props = new Properties();
        props.load(is);
        logFile = props.getProperty(LOG4J_APPENDER_STREAMSETS_FILE);
        if (logFile != null) {
          logFile = resolveValue(logFile);
        }
      } catch (Exception ex) {
        logFile = null;
        LOG.error("Could not determine log file, {}", ex.getMessage(), ex);
      }
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

  static String resolveValue(String str) {
    while (str.contains("${")) {
      int start = str.indexOf("${");
      int end = str.indexOf("}", start);
      String value = System.getProperty(str.substring(start + 2, end));
      String current = str;
      str = current.substring(0, start) + value + current.substring(end + 1);
    }
    return str;
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
      IOException {
    if (request.isUserInRole(AuthzRole.ADMIN) || request.isUserInRole(AuthzRole.MANAGER)) {
      super.service(request, response);
    } else {
      response.sendError(HttpServletResponse.SC_FORBIDDEN);
    }
  }
}
