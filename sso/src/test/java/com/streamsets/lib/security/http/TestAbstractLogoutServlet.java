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

import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class TestAbstractLogoutServlet {

  @Test
  public void testServlet() throws Exception {
    final SSOService ssoService = Mockito.mock(SSOService.class);

    AbstractLogoutServlet servlet = new AbstractLogoutServlet() {
      @Override
      protected SSOService getSsoService() {
        return ssoService;
      }
    };

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    // no login cookie
    servlet.doGet(req, res);
    Mockito.verify(ssoService, Mockito.times(0)).invalidateAppToken(Mockito.anyString());

    // login cookie
    Cookie loginCookie = new Cookie(HttpUtils.getLoginCookieName(), "v");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {loginCookie} );
    servlet.doGet(req, res);
    Mockito.verify(ssoService, Mockito.times(1)).invalidateUserToken(Mockito.eq("v"));

    // no rest call
    Mockito.reset(req);
    Mockito.reset(res);
    servlet.doGet(req, res);
    Mockito.verify(res, Mockito.times(1)).sendRedirect(Mockito.eq(AbstractLoginServlet.URL_PATH));

    // rest call
    Mockito.reset(req);
    Mockito.reset(res);

    servlet.doGet(req, res);
    Mockito.verify(res, Mockito.times(1)).sendRedirect(Mockito.eq(AbstractLoginServlet.URL_PATH));

  }
}
