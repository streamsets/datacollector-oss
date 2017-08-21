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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class TestAbstractLoginServlet {

  @Test
  public void testCreateRedirectionUrl() throws Exception {
    AbstractLoginServlet servlet = new AbstractLoginServlet() {
      @Override
      protected SSOService getSsoService() {
        return null;
      }

      @Override
      protected String getLoginPage() {
        return "login.html";
      }
    };

    String got = servlet.createRedirectionUrl("r", "t");
    Assert.assertEquals(
        "r?" + SSOConstants.USER_AUTH_TOKEN_PARAM + "=t&" + SSOConstants.REPEATED_REDIRECT_PARAM + "=", got
    );

    got = servlet.createRedirectionUrl("r?a=A", "t");
    Assert.assertEquals(
        "r?a=A&" + SSOConstants.USER_AUTH_TOKEN_PARAM + "=t&" + SSOConstants.REPEATED_REDIRECT_PARAM + "=", got
    );
  }

  @Test
  public void testDispatchToLoginPage() throws Exception {
    AbstractLoginServlet servlet = new AbstractLoginServlet() {
      @Override
      protected SSOService getSsoService() {
        return null;
      }

      @Override
      protected String getLoginPage() {
        return "/login.html";
      }
    };
    servlet = Mockito.spy(servlet);
    ServletContext context = Mockito.mock(ServletContext.class);
    RequestDispatcher dispatcher = Mockito.mock(RequestDispatcher.class);
    Mockito.doReturn(dispatcher).when(context).getRequestDispatcher(Mockito.anyString());
    Mockito.doReturn(context).when(servlet).getServletContext();
    Mockito.doReturn(context).when(context).getContext(Mockito.eq("/"));

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getQueryString()).thenReturn("qs");
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    servlet.dispatchToLoginPage(req, res);
    ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
    Mockito.verify(context, Mockito.times(1)).getRequestDispatcher(url.capture());
    Assert.assertEquals("/login.html?qs", url.getValue());
    Mockito.verify(dispatcher, Mockito.times(1)).forward(Mockito.eq(req), Mockito.eq(res));
  }
  @Test
  public void testServlet() throws Exception {
    final SSOService ssoService = Mockito.mock(SSOService.class);

    AbstractLoginServlet servlet = new AbstractLoginServlet() {
      @Override
      protected SSOService getSsoService() {
        return ssoService;
      }

      @Override
      protected String getLoginPage() {
        return "login.html";
      }
    };
    servlet = Mockito.spy(servlet);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    // no login cookie
    Mockito.doNothing().when(servlet).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));
    servlet.doGet(req, res);
    Mockito.verify(servlet, Mockito.times(1)).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));

    // login cookie, invalid principal
    Mockito.reset(servlet);
    Mockito.reset(req);
    Mockito.reset(res);
    Mockito.reset(ssoService);
    Mockito.doNothing().when(servlet).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));
    Cookie loginCookie = new Cookie(HttpUtils.getLoginCookieName(), "v");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {loginCookie} );
    Mockito.when(ssoService.validateUserToken(Mockito.eq("v"))).thenReturn(null);
    servlet.doGet(req, res);
    Mockito.verify(servlet, Mockito.times(1)).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));

    // login cookie, valid principal, no redirect param
    Mockito.reset(servlet);
    Mockito.reset(req);
    Mockito.reset(res);
    Mockito.reset(ssoService);
    Mockito.doNothing().when(servlet).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));
    loginCookie = new Cookie(HttpUtils.getLoginCookieName(), "v");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {loginCookie} );
    Mockito.when(ssoService.validateUserToken(Mockito.eq("v"))).thenReturn(Mockito.mock(SSOPrincipal.class));
    servlet.doGet(req, res);
    Mockito.verify(res, Mockito.times(1)).setHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN), Mockito.eq("v"));
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_ACCEPTED));
    Mockito.verify(servlet, Mockito.never()).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));

    // login cookie, valid principal, redirect param
    Mockito.reset(servlet);
    Mockito.reset(req);
    Mockito.reset(res);
    Mockito.reset(ssoService);
    Mockito.doNothing().when(servlet).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));
    loginCookie = new Cookie(HttpUtils.getLoginCookieName(), "v");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {loginCookie} );
    Mockito.when(req.getParameter(Mockito.eq(SSOConstants.REQUESTED_URL_PARAM))).thenReturn("redirUrl");
    Mockito.when(ssoService.validateUserToken(Mockito.eq("v"))).thenReturn(Mockito.mock(SSOPrincipal.class));
    servlet.doGet(req, res);
    Mockito.verify(res, Mockito.times(1)).setHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN), Mockito.eq("v"));
    ArgumentCaptor<String> redir = ArgumentCaptor.forClass(String.class);
    Mockito.verify(res, Mockito.times(1)).sendRedirect(redir.capture());
    Assert.assertTrue(redir.getValue().startsWith("redirUrl"));
    Mockito.verify(servlet, Mockito.never()).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));

    // login cookie, valid principal, repeated redir param
    Mockito.reset(servlet);
    Mockito.reset(req);
    Mockito.reset(res);
    Mockito.reset(ssoService);
    Mockito.doNothing().when(servlet).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));
    loginCookie = new Cookie(HttpUtils.getLoginCookieName(), "v");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {loginCookie} );
    Mockito.when(req.getParameter(Mockito.eq(SSOConstants.REQUESTED_URL_PARAM))).thenReturn("redirUrl");
    Mockito.when(req.getParameter(Mockito.eq(SSOConstants.REPEATED_REDIRECT_PARAM))).thenReturn("x");
    Mockito.when(ssoService.validateUserToken(Mockito.eq("v"))).thenReturn(Mockito.mock(SSOPrincipal.class));
    servlet.doGet(req, res);
    Mockito.verify(res, Mockito.times(1)).setHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN), Mockito.eq("v"));
    Mockito.verify(ssoService, Mockito.times(1)).invalidateUserToken(Mockito.eq("v"));
    Mockito.verify(servlet, Mockito.times(1)).dispatchToLoginPage(Mockito.eq(req), Mockito.eq(res));
  }
}
