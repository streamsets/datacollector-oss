/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.lib.security.http;

import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.ThreadPool;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LimitedMethodServer extends Server {
  private List<String> prohibitedMethods = Arrays.asList("TRACE", "TRACK");

  public LimitedMethodServer(ThreadPool threadPool) {
    super(threadPool);
  }

  @Override
  public void handle(HttpChannel channel) throws IOException, ServletException {
    if(prohibitedMethods.contains(channel.getRequest().getMethod().toUpperCase())) {
      channel.getRequest().setHandled(true);
      channel.getResponse().setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    } else {
      super.handle(channel);
    }
  }
}
