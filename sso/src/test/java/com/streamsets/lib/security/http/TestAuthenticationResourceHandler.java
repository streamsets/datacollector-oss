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
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

public class TestAuthenticationResourceHandler {

  @Test
  public void testLogin() {
    Authentication authentication = Mockito.mock(Authentication.class);

    LoginJson login = new LoginJson();
    login.setUserName("u");
    login.setPassword("p");

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRemoteAddr()).thenReturn("ip");

    // no authentication principal

    Mockito
        .when(authentication.validateUserCredentials(Mockito.eq("u"), Mockito.eq("p"), Mockito.eq("ip")))
        .thenReturn(null);

    AuthenticationResourceHandler handler = new AuthenticationResourceHandler(authentication, false);
    handler = Mockito.spy(handler);

    Response response = handler.login(req, login);
    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    Assert.assertEquals(AuthenticationResourceHandler.AUTHENTICATION_FAILED, response.getEntity());

    // authentication principal
    SSOPrincipalJson principal = new SSOPrincipalJson();
    principal.setTokenStr("token");

    Mockito
        .when(authentication.validateUserCredentials(Mockito.eq("u"), Mockito.eq("p"), Mockito.eq("ip")))
        .thenReturn(principal);

    response = handler.login(req, login);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals("token", response.getHeaderString(SSOConstants.X_USER_AUTH_TOKEN));
    Assert.assertEquals("token", response.getCookies().values().iterator().next().getValue());
    Mockito.verify(authentication, Mockito.times(1)).registerSession(Mockito.eq(principal));
    Mockito.verify(handler, Mockito.times(1)).createLoginCookie(Mockito.eq(req), Mockito.eq(principal));
  }

  @Test
  public void testCreateLoginCookie() {
    // no secure load balancer
    AuthenticationResourceHandler handler = new AuthenticationResourceHandler(null, false);
    handler = Mockito.spy(handler);
    Mockito.doReturn(1L).when(handler).getTimeNow();

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    SSOPrincipalJson principal = new SSOPrincipalJson();
    principal.setTokenStr("token");

    // session cookie
    principal.setExpires(-1);

    // request not secure
    Mockito.doReturn(false).when(req).isSecure();

    NewCookie cookie = handler.createLoginCookie(req, principal);
    Assert.assertEquals(HttpUtils.getLoginCookieName(), cookie.getName());
    Assert.assertEquals("token", cookie.getValue());
    Assert.assertEquals("/", cookie.getPath());
    Assert.assertEquals(NewCookie.DEFAULT_MAX_AGE, cookie.getMaxAge());
    Assert.assertFalse(cookie.isSecure());

    // persistent cookie
    principal.setExpires(2001);
    cookie = handler.createLoginCookie(req, principal);
    Assert.assertEquals(2, cookie.getMaxAge());

    // request secure
    Mockito.doReturn(true).when(req).isSecure();
    cookie = handler.createLoginCookie(req, principal);
    Assert.assertTrue(cookie.isSecure());

    // secure load balancer
    handler = new AuthenticationResourceHandler(null, true);
    handler = Mockito.spy(handler);
    Mockito.doReturn(1L).when(handler).getTimeNow();

    // request not secure
    Mockito.doReturn(false).when(req).isSecure();
    cookie = handler.createLoginCookie(req, principal);
    Assert.assertTrue(cookie.isSecure());

    // request secure
    Mockito.doReturn(true).when(req).isSecure();
    cookie = handler.createLoginCookie(req, principal);
    Assert.assertTrue(cookie.isSecure());
  }

}
