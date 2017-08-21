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
package com.streamsets.datacollector.http;

import org.eclipse.jetty.security.authentication.FormAuthenticator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

public class LoginServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
    HttpSession session = req.getSession();
    String user = req.getParameter("j_username");
    String pass = req.getParameter("j_password");
    String basePath = req.getParameter("basePath");

    if(basePath == null || basePath.trim().length() == 0) {
      basePath = "/";
    }

    String redirectURL = (String)session.getAttribute(FormAuthenticator.__J_URI);
    if((redirectURL != null && redirectURL.contains("rest/v1/")) || !basePath.equals("/")) {
      session.setAttribute(FormAuthenticator.__J_URI, basePath);
    }

    resp.sendRedirect(basePath + "j_security_check?j_username=" + user + "&j_password=" + pass);
  }
}
