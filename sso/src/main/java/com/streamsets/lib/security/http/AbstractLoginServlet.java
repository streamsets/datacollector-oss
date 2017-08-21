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
import java.net.URLEncoder;


public abstract class AbstractLoginServlet extends AbstractAuthHttpServlet {

  public static final String URL_PATH = "/security/login";

  private static final Logger LOG = LoggerFactory.getLogger(AbstractLoginServlet.class);

  protected abstract String getLoginPage();

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    boolean toLoginPage = true;
    Cookie loginCookie = HttpUtils.getLoginCookie(req);
    if (loginCookie != null) {
      LOG.debug("Request has a '{}' cookie", HttpUtils.getLoginCookieName());

      String tokenStr = loginCookie.getValue();
      if (tokenStr != null) {
        SSOPrincipal principal = getSsoService().validateUserToken(tokenStr);
        if (principal != null) {
          LOG.debug("Request already has an authenticated user '{}', skipping login page", principal.getName());
          res.setHeader(SSOConstants.X_USER_AUTH_TOKEN, tokenStr);
          String redirectUrl = req.getParameter(SSOConstants.REQUESTED_URL_PARAM);
          if (redirectUrl != null) {
            boolean repeatedRedirect = req.getParameter(SSOConstants.REPEATED_REDIRECT_PARAM) != null;
            if (repeatedRedirect) {
              LOG.warn("Request is a repeated redirect, invalidating token '{}'", tokenStr);
              getSsoService().invalidateUserToken(tokenStr);
            } else {
              LOG.debug("Redirecting back to '{}'", redirectUrl);
              redirectUrl = createRedirectionUrl(redirectUrl, tokenStr);
              res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
              res.setHeader("Pragma", "no-cache");
              res.setHeader("Expires", "0");
              res.sendRedirect(redirectUrl);
              toLoginPage = false;
            }
          } else {
            res.setStatus(HttpServletResponse.SC_ACCEPTED);
            toLoginPage = false;
          }
        } else {
          LOG.debug("Request has an invalid '{}' cookie '{}'", HttpUtils.getLoginCookieName(), tokenStr);
        }
      }
    }

    if (toLoginPage) {
      dispatchToLoginPage(req, res);
    }
  }

  String createRedirectionUrl(String redirectUrl, String tokenStr) throws IOException {
    StringBuilder sb = new StringBuilder(redirectUrl);
    if (redirectUrl.contains("?")) {
      sb.append("&");
    } else {
      sb.append("?");
    }
    sb.append(SSOConstants.USER_AUTH_TOKEN_PARAM)
        .append("=")
        .append(URLEncoder.encode(tokenStr, "UTF-8"))
        .append("&")
        .append(SSOConstants.REPEATED_REDIRECT_PARAM)
        .append("=");
    return sb.toString();
  }

  void dispatchToLoginPage(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException {
    StringBuilder sb = new StringBuilder(getLoginPage());
    String queryString = req.getQueryString();
    if (queryString != null) {
      sb.append("?").append(queryString);
    }
    getServletContext().getContext("/").getRequestDispatcher(sb.toString()).forward(req, res);
  }
}
