/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.event.handler.remote;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

public class MockServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (req.getRequestURI().contains("availableApps")) {
      resp.setContentType("application/json");
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write(ObjectMapperFactory.get().writeValueAsString(
          ImmutableList.of(WebSocketToRestDispatcher.TUNNELING_APP_NAME))
      );
    } else if (req.getHeader("mockHeader") != null) {
      resp.setContentType(MediaType.TEXT_PLAIN);
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write("Plain text output payload");
    } else {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.setContentType("application/json");
    if (req.getHeader("mockHeader") != null) {
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter()
          .write(ObjectMapperFactory.get().writeValueAsString(ImmutableMap.of("k1", "v1")));
    } else {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
}
