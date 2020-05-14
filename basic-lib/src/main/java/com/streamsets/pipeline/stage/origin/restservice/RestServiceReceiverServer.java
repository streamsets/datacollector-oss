/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.restservice;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.AbstractHttpReceiverServer;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.httpsource.HttpSourceConfigs;
import com.streamsets.pipeline.stage.origin.httpserver.HttpReceiverServerPush;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.concurrent.BlockingQueue;

public class RestServiceReceiverServer extends AbstractHttpReceiverServer {

  private final HttpReceiver receiver;
  private RestServiceReceiverServlet servlet;

  RestServiceReceiverServer(HttpConfigs configs, HttpReceiver receiver, BlockingQueue<Exception> errorQueue) {
    super(configs, errorQueue);
    this.receiver = receiver;
  }

  @Override
  public void addReceiverServlet(Stage.Context context, ServletContextHandler contextHandler) {
    servlet = new RestServiceReceiverServlet(context, receiver, errorQueue);
    contextHandler.addServlet(new ServletHolder(servlet), receiver.getUriPath());
    HttpSourceConfigs httpSourceConfigs = (HttpSourceConfigs) configs;

    SecurityHandler securityHandler = null;
    if (httpSourceConfigs.spnegoConfigBean.isSpnegoEnabled()) {
      securityHandler = HttpReceiverServerPush.getSpnegoAuthHandler(httpSourceConfigs, context);
    } else if (httpSourceConfigs.tlsConfigBean.isEnabled()) {
      securityHandler = HttpReceiverServerPush.getBasicAuthHandler(httpSourceConfigs);
    }
    if (securityHandler != null) {
      contextHandler.setSecurityHandler(securityHandler);
    }
  }

  @Override
  public void setShuttingDown() {
    servlet.setShuttingDown();
  }

}
