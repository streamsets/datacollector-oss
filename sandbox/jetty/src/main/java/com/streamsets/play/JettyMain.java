/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
