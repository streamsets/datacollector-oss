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
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.Collections;

public class TestSSOAuthenticator {

  @Test
  public void testConstructor() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    new SSOAuthenticator("a", ssoService);
    Mockito.verify(ssoService).setListener(Mockito.<SSOService.Listener>any());
  }

  @Test
  public void testGetRequestUrl() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

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

    Assert.assertEquals("http://foo/bar?a=A&b=B&c=C", authenticator.getRequestUrl(req).toString());

    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&" + SSOConstants.USER_AUTH_TOKEN_PARAM + "=token");

    Assert.assertEquals("http://foo/bar?a=A&b=B", authenticator.getRequestUrlWithoutToken(req).toString());
  }

  @Test
  public void testGetLoginUrl() throws Exception {
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

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
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

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
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

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
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
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
    Mockito.verify(res).sendError(error.capture());
    Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN, error.getValue().intValue());
    if (rest) {
      Assert.assertEquals(SSOAuthenticator.FORBIDDEN_JSON_STR, writer.toString().trim());
      Mockito.verify(res).setContentType(Mockito.eq("application/json"));
    } else {
      Assert.assertTrue(writer.toString().isEmpty());
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
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");
    Mockito.when(req.getMethod()).thenReturn("POST");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.handleRequestWithoutUserAuthHeader(req, res));
    ArgumentCaptor<Integer> error = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(res).sendError(error.capture());
    Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN, error.getValue().intValue());
  }

  @Test
  public void testHandleRequestWithoutUserAuthHeaderPageWithGET() throws Exception {
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

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
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

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
    SSOService ssoService = Mockito.spy(new RemoteSSOService(new Configuration()));
    SSOTokenParser parser = Mockito.mock(SSOTokenParser.class);
    Mockito.when(parser.parse(Mockito.anyString())).thenReturn(TestSSOUserToken.createToken());
    Mockito.when(ssoService.getTokenParser()).thenReturn(parser);
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&" + SSOConstants.USER_AUTH_TOKEN_PARAM + "=token");
    Mockito.when(req.getParameter(Mockito.eq(SSOConstants.USER_AUTH_TOKEN_PARAM))).thenReturn("token");
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getSession(Mockito.eq(true))).thenReturn(Mockito.mock(HttpSession.class));

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.SEND_CONTINUE, authenticator.handleRequestWithoutUserAuthHeader(req, res));
    Mockito.verify(res).sendRedirect(Mockito.anyString());
  }

  @Test
  public void testHandleRequiresAuthenticationPage() throws Exception {
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

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
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

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

  @Test
  public void testProcessTokenOK() throws Exception {
    SSOService ssoService = Mockito.spy(new RemoteSSOService(new Configuration()));
    SSOTokenParser parser = Mockito.mock(SSOTokenParser.class);
    Mockito.when(parser.parse(Mockito.anyString())).thenReturn(TestSSOUserToken.createToken());
    Mockito.when(ssoService.getTokenParser()).thenReturn(parser);
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpSession session = Mockito.mock(HttpSession.class);
    Mockito.when(req.getSession(Mockito.eq(true))).thenReturn(session);

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertFalse(authenticator.isKnownToken(SSOAuthenticationUser.createForInvalidation(TestSSOUserToken.TOKEN_ID)));
    Authentication user = authenticator.processToken("token", req, res);
    Assert.assertTrue(user instanceof SSOAuthenticationUser);
    Assert.assertTrue(authenticator.isKnownToken(SSOAuthenticationUser.createForInvalidation(TestSSOUserToken.TOKEN_ID)));
    ArgumentCaptor<Object> capture = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(session).setAttribute(Mockito.eq(SessionAuthentication.__J_AUTHENTICATED), capture.capture());
    Assert.assertEquals(user, capture.getValue());
  }

  @Test
  public void testProcessTokenFail() throws Exception {
    SSOService ssoService = Mockito.spy(new RemoteSSOService(new Configuration()));
    SSOTokenParser parser = Mockito.mock(SSOTokenParser.class);
    Mockito.when(parser.parse(Mockito.anyString())).thenReturn(null);
    Mockito.when(ssoService.getTokenParser()).thenReturn(parser);
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertFalse(authenticator.isKnownToken(SSOAuthenticationUser.createForInvalidation(TestSSOUserToken.TOKEN_ID)));
    Authentication user = authenticator.processToken("token", req, res);
    Assert.assertFalse(authenticator.isKnownToken(SSOAuthenticationUser.createForInvalidation(TestSSOUserToken.TOKEN_ID)));
    ArgumentCaptor<Integer> error = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(res).sendError(error.capture());
    Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN, error.getValue().intValue());
    Assert.assertEquals(Authentication.SEND_FAILURE, user);
  }

  @Test
  public void testSetGetAuthenticationUser() {
    SSOService ssoService = new RemoteSSOService(new Configuration());
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);

    HttpSession session = Mockito.mock(HttpSession.class);
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);

    authenticator.setAuthenticationUser(session, user);

    // set
    Mockito.verify(session).setAttribute(Mockito.eq(SessionAuthentication.__J_AUTHENTICATED), Mockito.eq(user));

    // get null session
    Assert.assertNull(authenticator.getAuthenticationUser(null));

    // get not-null session
    session = Mockito.mock(HttpSession.class);
    Mockito.when(session.getAttribute(Mockito.eq(SessionAuthentication.__J_AUTHENTICATED))).thenReturn(user);

    Assert.assertEquals(user, authenticator.getAuthenticationUser(session));
  }

  @Test
  public void testValidateRequestNotAuthenticatedNoHeaderNotMandatory() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOAuthenticator authenticator = Mockito.spy(new SSOAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(null).when(authenticator).getAuthenticationUser(Mockito.<HttpSession>any());

    Assert.assertEquals(Authentication.NOT_CHECKED, authenticator.validateRequest(req, res, false));

  }

  @Test
  public void testValidateRequestNotAuthenticatedNoAuthHeaderMandatory() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOAuthenticator authenticator = Mockito.spy(new SSOAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    Authentication dummyAuth = new Authentication() {
    };

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(null).when(authenticator).getAuthenticationUser(Mockito.<HttpSession>any());
    Mockito.doReturn(dummyAuth)
        .when(authenticator)
        .handleRequestWithoutUserAuthHeader(Mockito.eq(req), Mockito.eq(res));

    Assert.assertEquals(dummyAuth, authenticator.validateRequest(req, res, true));
  }

  @Test
  public void testValidateRequestNotAuthenticatedAuthHeaderMandatory() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOAuthenticator authenticator = Mockito.spy(new SSOAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    Authentication dummyAuth = new Authentication() {
    };

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("token");
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(null).when(authenticator).getAuthenticationUser(Mockito.<HttpSession>any());
    Mockito.doReturn(dummyAuth)
        .when(authenticator)
        .processToken(Mockito.eq("token"), Mockito.eq(req), Mockito.eq(res));

    Assert.assertEquals(dummyAuth, authenticator.validateRequest(req, res, true));
  }

  @Test
  public void testValidateRequestAuthenticatedUserValid() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOAuthenticator authenticator = Mockito.spy(new SSOAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    SSOUserToken token = Mockito.mock(SSOUserToken.class);
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(user.getToken()).thenReturn(token);
    Mockito.when(user.isValid()).thenReturn(true);
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(user).when(authenticator).getAuthenticationUser(Mockito.<HttpSession>any());

    Assert.assertEquals(user, authenticator.validateRequest(req, res, true));
  }

  @Test
  public void testValidateRequestAuthenticatedUserInvalid() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOAuthenticator authenticator = Mockito.spy(new SSOAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    SSOUserToken token = Mockito.mock(SSOUserToken.class);
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(user.getToken()).thenReturn(token);
    Mockito.when(user.isValid()).thenReturn(false);

    Authentication dummyAuth = new Authentication() {
    };

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpSession session = Mockito.mock(HttpSession.class);
    Mockito.when(req.getSession(Mockito.eq(false))).thenReturn(session);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(user).when(authenticator).getAuthenticationUser(Mockito.eq(session));
    Mockito.doReturn(dummyAuth).when(authenticator).handleRequiresAuthentication(Mockito.eq(req), Mockito.eq(res));

    Assert.assertEquals(dummyAuth, authenticator.validateRequest(req, res, true));
    Mockito.verify(session).invalidate();
  }

  @Test
  public void testValidateRequestAuthenticatedButDifferentTokenInRequest() throws Exception {
    SSOService ssoService = Mockito.spy(SSOService.class);
    SSOAuthenticator authenticator = Mockito.spy(new SSOAuthenticator("a", ssoService));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.<HttpServletRequest>any());

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN))).thenReturn("foo");

    SSOUserToken token = Mockito.mock(SSOUserToken.class);
    Mockito.when(token.getRawToken()).thenReturn("bar");
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(user.getToken()).thenReturn(token);
    HttpSession session = Mockito.mock(HttpSession.class);
    Mockito.when(req.getSession(Mockito.eq(false))).thenReturn(session);
    Mockito.doReturn(user).when(authenticator).getAuthenticationUser(Mockito.eq(session));


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
      public void init() {
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
    };

    invalidationListener = null;
    SSOAuthenticator authenticator = new SSOAuthenticator("a", ssoService);
    Assert.assertNotNull(invalidationListener);

    HttpSession session = Mockito.mock(HttpSession.class);
    SSOAuthenticationUser user = SSOAuthenticationUser.createForInvalidation("a");

    Assert.assertFalse(authenticator.isKnownToken(user));
    authenticator.registerToken(session, user);
    Assert.assertTrue(authenticator.isKnownToken(user));

    Mockito.verify(session, Mockito.never()).invalidate();

    invalidationListener.invalidate(ImmutableList.of("a"));
    Assert.assertFalse(authenticator.isKnownToken(user));
    Mockito.verify(session).invalidate();
  }

  @Test
  public void testIsLogoutRequest() throws Exception {
    SSOAuthenticator authenticator = new SSOAuthenticator("a", Mockito.spy(SSOService.class));

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
    SSOAuthenticator authenticator = Mockito.spy(new SSOAuthenticator("a", ssoService));
    Mockito.doNothing().when(authenticator).invalidateToken(Mockito.anyString());

    SSOUserToken token = Mockito.mock(SSOUserToken.class);
    SSOAuthenticationUser user = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(user.getToken()).thenReturn(token);
    Mockito.when(user.isValid()).thenReturn(true);
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer());
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(user).when(authenticator).getAuthenticationUser(Mockito.<HttpSession>any());

    Mockito.when(req.getContextPath()).thenReturn("/foo");
    Mockito.when(req.getMethod()).thenReturn("POST");
    Mockito.when(req.getRequestURI()).thenReturn("/foo/logout");

    Assert.assertEquals(Authentication.SEND_SUCCESS, authenticator.validateRequest(req, res, true));

    Mockito.verify(authenticator).isLogoutRequest(Mockito.eq(req));
    Mockito.verify(authenticator).invalidateToken(Mockito.anyString());

  }

}
