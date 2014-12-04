/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.play;


import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class JettyMain {

  public static class PingServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      resp.setContentType("text/plain");
      resp.getWriter().write("pong\n");
    }
  }

  public static void main(String[] args) throws Exception {
    Server server = new Server(8080);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    MetricRegistry registry = new MetricRegistry();
    JmxReporter reporter = JmxReporter.forRegistry(registry).build();
    reporter.start();
    context.setAttribute("com.codahale.metrics.servlets.MetricsServlet.registry", registry);
    context.addServlet(new ServletHolder(new PingServlet()), "/ping");
    context.addServlet(new ServletHolder(new JMXJsonServlet()), "/jmx");
    server.setHandler(context);
    server.start();
    server.join();
  }
}
