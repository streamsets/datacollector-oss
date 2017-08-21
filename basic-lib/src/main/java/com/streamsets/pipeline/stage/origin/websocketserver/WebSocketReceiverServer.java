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
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.AbstractHttpReceiverServer;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.concurrent.BlockingQueue;

public class WebSocketReceiverServer extends AbstractHttpReceiverServer {

  private final WebSocketReceiver receiver;
  private WebSocketReceiverServlet servlet;

  WebSocketReceiverServer(HttpConfigs configs, WebSocketReceiver receiver, BlockingQueue<Exception> errorQueue) {
    super(configs, errorQueue);
    this.receiver = receiver;
  }

  @Override
  public void addReceiverServlet(Stage.Context context, ServletContextHandler contextHandler) {
    servlet = new WebSocketReceiverServlet(context, receiver, errorQueue);
    contextHandler.addServlet(new ServletHolder(servlet), receiver.getUriPath());
  }

  @Override
  public void setShuttingDown() {
    servlet.setShuttingDown();
  }

}
