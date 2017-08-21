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
package com.streamsets.lib.security.http;


import com.streamsets.lib.security.http.AbstractLogoutServlet;
import com.streamsets.lib.security.http.DisconnectedSSOService;
import com.streamsets.lib.security.http.SSOService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class DisconnectedLogoutServlet extends AbstractLogoutServlet {
  private final DisconnectedSSOService service;

  public DisconnectedLogoutServlet(DisconnectedSSOService service) {
    this.service = service;
  }

  @Override
  protected SSOService getSsoService() {
    return service;
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (service.isEnabled()) {
      super.service(req, resp);
    } else {
      resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }
  }

}
