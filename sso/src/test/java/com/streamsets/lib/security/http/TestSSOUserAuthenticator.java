/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.util.Configuration;
import org.eclipse.jetty.server.Authentication;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.Collections;

public class TestSSOUserAuthenticator {

  @Test
  public void testConstructor() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    new SSOUserAuthenticator("a", ssoService);
    Mockito.verify(ssoService).setListener(Mockito.<SSOService.Listener>any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetRequestUrl() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn(null);

    Assert.assertEquals("http://foo/bar", authenticator.getRequestUrl(req, Collections.EMPTY_SET).toString());

    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&c=C");

    Assert.assertEquals("http://foo/bar?a=A&b=B&c=C",
        authenticator.getRequestUrl(req, Collections.EMPTY_SET).toString()
    );

    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&c=C");

    Assert.assertEquals("http://foo/bar?a=A&c=C", authenticator.getRequestUrl(req, ImmutableSet.of("b")).toString());


    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&c=C");

    Assert.assertEquals("http://foo/bar?a=A&b=B&c=C", authenticator.getRequestUrl(req));

    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&" + SSOConstants.USER_AUTH_TOKEN_PARAM + "=token");

    Assert.assertEquals("http://foo/bar?a=A&b=B", authenticator.getRequestUrlWithoutToken(req));
  }

  @Test
  public void testGetLoginUrl() throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn(null);

    Assert.assertEquals(
        "http://localhost:18631/security/login?" + SSOConstants.REQUESTED_URL_PARAM + "=" + URLEncoder.encode
            ("http://foo/bar", "UTF-8"),
        authenticator.getLoginUrl(req)
    );
  }

  @Test
  public void testRedirectToSelf() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&" + SSOConstants.USER_AUTH_TOKEN_PARAM + "=token");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.SEND_CONTINUE, authenticator.redirectToSelf(req, res));
    ArgumentCaptor<String> redirect = ArgumentCaptor.forClass(String.class);
    Mockito.verify(res).sendRedirect(redirect.capture());
    Assert.assertEquals("http://foo/bar?a=A&b=B", redirect.getValue());
  }

  @Test
  public void testRedirectToLogin() throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.SEND_CONTINUE, authenticator.redirectToLogin(req, res));
    ArgumentCaptor<String> redirect = ArgumentCaptor.forClass(String.class);
    Mockito.verify(res).sendRedirect(redirect.capture());
    Assert.assertEquals("http://localhost:18631/security/login?" +
        SSOConstants.REQUESTED_URL_PARAM +
        "=" +
        URLEncoder.encode("http://foo/bar?a=A&b=B", "UTF-8"), redirect.getValue());
  }

  private void testReturnForbidden(boolean rest) throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return new StringBuffer("http://foo/bar");
      }
    }).when(req).getRequestURL();
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");
    if (rest) {
      Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_REST_CALL))).thenReturn("foo");
    }

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    Mockito.when(res.getWriter()).thenReturn(printWriter);

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.returnForbidden(req, res, ""));

    ArgumentCaptor<Integer> error = ArgumentCaptor.forClass(Integer.class);
    if (rest) {
      Mockito.verify(res).sendError(error.capture());
      Assert.assertEquals(SSOUserAuthenticator.FORBIDDEN_JSON_STR, writer.toString().trim());
      Mockito.verify(res).setContentType(Mockito.eq("application/json"));
    } else {
      ArgumentCaptor<String> redirect = ArgumentCaptor.forClass(String.class);
      Mockito.verify(res).sendRedirect(redirect.capture());
      Assert.assertEquals("http://localhost:18631/security/login?" +
          SSOConstants.REQUESTED_URL_PARAM +
          "=" +
          URLEncoder.encode("http://foo/bar?a=A&b=B", "UTF-8"), redirect.getValue());
    }
  }

  @Test
  public void testReturnForbiddenPage() throws Exception {
    testReturnForbidden(false);
  }

  @Test
  public void testReturnForbiddenRest() throws Exception {
    testReturnForbidden(true);
  }

  @Test
  public void testHandleRequestWithoutUserAuthHeaderOtherThanGET() throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");
    Mockito.when(req.getMethod()).thenReturn("POST");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.handleRequestWithoutUserAuthHeader(req, res));
    Mockito.verify(res).sendRedirect(Mockito.anyString());
  }

  @Test
  public void testHandleRequestWithoutUserAuthHeaderPageWithGET() throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");
    Mockito.when(req.getMethod()).thenReturn("GET");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.SEND_CONTINUE, authenticator.handleRequestWithoutUserAuthHeader(req, res));
    Mockito.verify(res).sendRedirect(Mockito.anyString());
  }

  @Test
  public void testHandleRequestWithoutUserAuthHeaderRESTWithGET() throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_REST_CALL))).thenReturn("true");
    Mockito.when(req.getMethod()).thenReturn("GET");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(res.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.handleRequestWithoutUserAuthHeader(req, res));
    ArgumentCaptor<Integer> error = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(res).sendError(error.capture());
    Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN, error.getValue().intValue());
  }

  @Test
  public void testHandleRequestWithoutUserAuthHeaderWithUserAuthParamPageWithGET() throws Exception {
    RemoteSSOService ssoService = Mockito.spy(new RemoteSSOService());
    ssoService.setConfiguration(new Configuration());
    SSOTokenParser parser = Mockito.mock(SSOTokenParser.class);
    Mockito.when(parser.parse(Mockito.anyString())).thenReturn(TestSSOUserPrincipalJson.createPrincipal());
    Mockito.when(ssoService.getTokenParser()).thenReturn(parser);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&" + SSOConstants.USER_AUTH_TOKEN_PARAM + "=token");
    Mockito.when(req.getParameter(Mockito.eq(SSOConstants.USER_AUTH_TOKEN_PARAM))).thenReturn("token");
    Mockito.when(req.getMethod()).thenReturn("GET");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.SEND_CONTINUE, authenticator.handleRequestWithoutUserAuthHeader(req, res));
    Mockito.verify(res).sendRedirect(Mockito.anyString());
  }

  @Test
  public void testHandleRequiresAuthenticationPage() throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");
    Mockito.when(req.getMethod()).thenReturn("GET");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.SEND_CONTINUE, authenticator.handleRequiresAuthentication(req, res));
    Mockito.verify(res).sendRedirect(Mockito.anyString());
  }

  @Test
  public void testHandleRequiresAuthenticationRest() throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_REST_CALL))).thenReturn("true");
    Mockito.when(req.getMethod()).thenReturn("GET");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(res.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.handleRequiresAuthentication(req, res));
    ArgumentCaptor<Integer> error = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(res).sendError(error.capture());
    Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN, error.getValue().intValue());

  }

  private static final String TOKEN_ID = "tokenId";

  @Test
  public void testProcessTokenOK() throws Exception {
    SSOService ssoService = Mockito.spy(new RemoteSSOService());
    ssoService.setConfiguration(new Configuration());
    SSOTokenParser parser = Mockito.mock(SSOTokenParser.class);
    Mockito.when(parser.parse(Mockito.anyString())).thenReturn(TestSSOUserPrincipalJson.createPrincipal());
    Mockito.when(ssoService.getTokenParser()).thenReturn(parser);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertFalse(authenticator.isKnownToken(TOKEN_ID));
    Authentication user = authenticator.processToken(TOKEN_ID, req, res);
    Assert.assertTrue(user instanceof SSOAuthenticationUser);
    Assert.assertTrue(authenticator.isKnownToken(TOKEN_ID));
  }

  @Test
  public void testProcessTokenFail() throws Exception {
    SSOService ssoService = Mockito.spy(new RemoteSSOService());
    ssoService.setConfiguration(new Configuration());
    SSOTokenParser parser = Mockito.mock(SSOTokenParser.class);
    Mockito.when(parser.parse(Mockito.anyString())).thenReturn(null);
    Mockito.when(ssoService.getTokenParser()).thenReturn(parser);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertFalse(authenticator.isKnownToken(TOKEN_ID));
    Authentication user = authenticator.processToken("token", req, res);
    Assert.assertFalse(authenticator.isKnownToken(TOKEN_ID));
    Mockito.verify(res).sendRedirect(Mockito.anyString());
    Assert.assertEquals(Authentication.SEND_FAILURE, user);
  }

  @Test
  public void testValidateRequestNotAuthenticatedNoHeaderNotMandatory() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    Mockito.when(req.getCookies()).thenReturn(null);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.NOT_CHECKED, authenticator.validateRequest(req, res, false));

  }

  @Test
  public void testValidateRequestNotAuthenticatedNoAuthHeaderMandatory() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    Authentication dummyAuth = new Authentication() {
    };

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    Mockito.when(req.getCookies()).thenReturn(null);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(dummyAuth)
        .when(authenticator)
        .handleRequestWithoutUserAuthHeader(Mockito.eq(req), Mockito.eq(res));

    Assert.assertEquals(dummyAuth, authenticator.validateRequest(req, res, true));
  }

  @Test
  public void testValidateRequestNotAuthenticatedAuthHeaderMandatory() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    Authentication dummyAuth = new Authentication() {
    };

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("token");
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(dummyAuth)
        .when(authenticator)
        .processToken(Mockito.eq("token"), Mockito.eq(req), Mockito.eq(res));

    Assert.assertEquals(dummyAuth, authenticator.validateRequest(req, res, true));
  }

  @Test
  public void testGetAuthCookieName() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getServerPort()).thenReturn(1);

    Assert.assertEquals(SSOConstants.AUTHENTICATION_COOKIE_PREFIX + "1", authenticator.getAuthCookieName(req));
  }

    @Test
  public void testGetAuthTokenFromRequest() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));
    Mockito.doReturn("XXX").when(authenticator).getAuthCookieName(Mockito.<HttpServletRequest>any());

    // in header
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("token");
    Assert.assertEquals("token", authenticator.getAuthTokenFromRequest(req));

    // in cookie
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn(null);
    Cookie cookie = new Cookie("XXX", "token");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] { cookie });
    Assert.assertEquals("token", authenticator.getAuthTokenFromRequest(req));

    // in both header and cookie

    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("tokenH");
    cookie = new Cookie("XXX", "tokenC");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] { cookie });
    Assert.assertEquals("tokenH", authenticator.getAuthTokenFromRequest(req));
  }

  @Test
  public void testValidateRequestAuthenticatedUserValid() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    SSOUserPrincipal token = Mockito.mock(SSOUserPrincipal.class);
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(user.getSSOUserPrincipal()).thenReturn(token);
    Mockito.when(user.isValid()).thenReturn(true);
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("token");
    Mockito.when(req.getCookies()).thenReturn(null);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(user).when(authenticator).getFromCache(Mockito.eq("token"));

    Assert.assertEquals(user, authenticator.validateRequest(req, res, true));
  }

  @Test
  public void testValidateRequestAuthenticatedUserInvalid() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    SSOUserPrincipal token = Mockito.mock(SSOUserPrincipal.class);
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(user.getSSOUserPrincipal()).thenReturn(token);
    Mockito.when(user.isValid()).thenReturn(false);

    Mockito.doReturn(user).when(authenticator).getFromCache(Mockito.eq("token"));
    Authentication dummyAuth = new Authentication() {
    };

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("token");
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    Mockito.when(req.getCookies()).thenReturn(null);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(dummyAuth).when(authenticator).handleRequiresAuthentication(Mockito.eq(req), Mockito.eq(res));

    Assert.assertEquals(dummyAuth, authenticator.validateRequest(req, res, true));
    Mockito.verify(authenticator).invalidateToken(Mockito.eq("token"));
    Mockito.verify(authenticator).createAuthCookie(Mockito.eq(req), Mockito.eq(""), Mockito.eq(0));
    Mockito.verify(res).addCookie(Mockito.any(Cookie.class));
  }

  @Test
  public void testValidateRequestAuthenticatedButDifferentTokenInRequest() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("foo");

    SSOUserPrincipal token = Mockito.mock(SSOUserPrincipal.class);
    Mockito.when(token.getTokenStr()).thenReturn("bar");
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(user.getSSOUserPrincipal()).thenReturn(token);

    Mockito.doNothing().when(authenticator).invalidateToken(Mockito.anyString());

    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Assert.assertEquals(Authentication.NOT_CHECKED, authenticator.validateRequest(req, res, false));
  }

  private SSOService.Listener invalidationListener;

  @Test
  public void testTokenInvalidation() throws Exception {
    SSOService ssoService = new SSOService() {

      @Override
      public void setDelegateTo(SSOService ssoService) {
      }

      @Override
      public void setConfiguration(Configuration conf) {
      }

      @Override
      public String createRedirectToLoginURL(String requestUrl) {
        return null;
      }

      @Override
      public SSOTokenParser getTokenParser() {
        return null;
      }

      @Override
      public void setListener(Listener listener) {
        invalidationListener = listener;
      }

      @Override
      public void refresh() {

      }

      @Override
      public boolean isAppAuthenticationEnabled() {
        return false;
      }

      @Override
      public SSOUserPrincipal validateAppToken(String authToken, String componentId) {
        return null;
      }

      @Override
      public long getValidateAppTokenFrequency() {
        return 0;
      }
    };

    invalidationListener = null;
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", ssoService);
    Assert.assertNotNull(invalidationListener);

    SSOAuthenticationUser user = new SSOAuthenticationUser(TestSSOUserPrincipalJson.createPrincipal());

    Assert.assertFalse(authenticator.isKnownToken(TOKEN_ID));
    authenticator.registerToken(TOKEN_ID, user);
    Assert.assertTrue(authenticator.isKnownToken(TOKEN_ID));

    invalidationListener.invalidate(ImmutableList.of(TOKEN_ID));
    Assert.assertFalse(authenticator.isKnownToken(TOKEN_ID));
  }

  @Test
  public void testIsLogoutRequest() throws Exception {
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator("a", Mockito.spy(SSOService.class));

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getContextPath()).thenReturn("/foo");
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getRequestURI()).thenReturn("/foo/bar");
    Assert.assertFalse(authenticator.isLogoutRequest(req));

    Mockito.when(req.getMethod()).thenReturn("POST");
    Assert.assertFalse(authenticator.isLogoutRequest(req));

    Mockito.when(req.getRequestURI()).thenReturn("/foo/logout");
    Assert.assertTrue(authenticator.isLogoutRequest(req));
  }

  @Test
  public void testLogout() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator("a", ssoService));
    Mockito.doNothing().when(authenticator).invalidateToken(Mockito.anyString());

    SSOUserPrincipal token = Mockito.mock(SSOUserPrincipal.class);
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(user.getSSOUserPrincipal()).thenReturn(token);
    Mockito.when(user.isValid()).thenReturn(true);
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(user).when(authenticator).getFromCache(Mockito.eq("foo"));

    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("foo");
    Mockito.when(req.getCookies()).thenReturn(null);
    Mockito.when(req.getContextPath()).thenReturn("/foo");
    Mockito.when(req.getMethod()).thenReturn("POST");
    Mockito.when(req.getRequestURI()).thenReturn("/foo/logout");

    Assert.assertEquals(Authentication.SEND_SUCCESS, authenticator.validateRequest(req, res, true));

    Mockito.verify(authenticator).isLogoutRequest(Mockito.eq(req));
    Mockito.verify(authenticator).invalidateToken(Mockito.anyString());

  }

}
