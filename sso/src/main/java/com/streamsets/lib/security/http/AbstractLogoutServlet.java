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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public abstract  class AbstractLogoutServlet extends AbstractAuthHttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractLogoutServlet.class);

  public static final String URL_PATH = "/security/_logout";

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    Cookie loginCookie = HttpUtils.getLoginCookie(req);
    if (loginCookie != null) {
      LOG.debug("Request has a '{}' cookie", HttpUtils.getLoginCookieName());
      String tokenStr = loginCookie.getValue();
      getSsoService().invalidateUserToken(tokenStr);
    }
    if (req.getHeader(SSOConstants.X_REST_CALL) != null) {
      // Logout is REST API call, Redirect to login page won't work
      res.setStatus(HttpServletResponse.SC_OK);
    } else {
      // non REST API calls are not expected, but we should handle them sending the user to the login page
      res.sendRedirect(AbstractLoginServlet.URL_PATH);
    }
  }
}
