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
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.servlet.DefaultServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.function.Supplier;

public class StaticWebServlet extends DefaultServlet {

  private final static String HEADLESS_URL = "/headless.html";
  private final static String HEADLESS_URL_TEMPLATE = HEADLESS_URL + "?url={}";
  private final static String ASSETS_FOLDER = "/assets/";
  private final RuntimeInfo runtimeInfo;

  public StaticWebServlet(RuntimeInfo runtimeInfo, Configuration conf) {
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    if (runtimeInfo.isDPMEnabled() &&
        runtimeInfo.isStaticWebDisabled() &&
        !request.getPathInfo().contains(HEADLESS_URL) &&
        !request.getPathInfo().contains(ASSETS_FOLDER)
    ) {
      String controlHubUrl = ((DpmClientInfo) runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY))
          .getDpmBaseUrl();
      String headlessHelperUrl = Utils.format(HEADLESS_URL_TEMPLATE, controlHubUrl);
      // When connected to the latest Control Hub instance, Data Collector functions as a headless engine without a UI.
      response.sendRedirect(headlessHelperUrl);
    } else {
      super.doGet(request, response);
    }
  }
}
