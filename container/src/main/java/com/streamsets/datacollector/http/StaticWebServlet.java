/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.datacollector.http;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import org.eclipse.jetty.servlet.DefaultServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class StaticWebServlet extends DefaultServlet {

  private final RuntimeInfo runtimeInfo;
  private final String controlHubUrl;

  public StaticWebServlet(RuntimeInfo runtimeInfo, Configuration conf) {
    this.controlHubUrl = RemoteSSOService.getValidURL(conf.get(
        RemoteSSOService.DPM_BASE_URL_CONFIG,
        RemoteSSOService.DPM_BASE_URL_DEFAULT
    ));
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    if (runtimeInfo.isDPMEnabled() && runtimeInfo.isStaticWebDisabled()) {
      // Redirect to Control Hub URL
      response.sendRedirect(this.controlHubUrl);
    } else {
      super.doGet(request, response);
    }
  }
}
