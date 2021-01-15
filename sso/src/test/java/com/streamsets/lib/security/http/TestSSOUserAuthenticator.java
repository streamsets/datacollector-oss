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

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.util.Configuration;
import org.eclipse.jetty.server.Authentication;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.Collections;

public class TestSSOUserAuthenticator {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @Test
  public void testConstructor() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator(ssoService, conf);
    Assert.assertEquals(ssoService, authenticator.getSsoService());
    Assert.assertNotNull(authenticator.getLog());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetRequestUrl() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator(ssoService, conf);

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
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "authToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "componentId");
    ssoService.setConfiguration(conf);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator(ssoService, conf);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn(null);

    Assert.assertEquals(
        "http://localhost:18631/security/login?" + SSOConstants.REQUESTED_URL_PARAM + "=" + URLEncoder.encode
            ("http://foo/bar", "UTF-8"),
        authenticator.getLoginUrl(req, false, null, null)
    );
    Assert.assertEquals(
        "http://localhost:18631/security/login?" + SSOConstants.REQUESTED_URL_PARAM + "=" + URLEncoder.encode
            ("http://foo/bar", "UTF-8") + "&" + SSOConstants.REPEATED_REDIRECT_PARAM + "=",
        authenticator.getLoginUrl(req, true, null, null)
    );
  }

  @Test
  public void testRedirectToSelf() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator(ssoService, conf);

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
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "authToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "componentId");
    ssoService.setConfiguration(conf);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator(ssoService, conf);

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

    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");
    Mockito.reset(res);
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B&" + SSOConstants.REPEATED_REDIRECT_PARAM + "=x");
    Mockito.when(req.getParameter(Mockito.eq(SSOConstants.REPEATED_REDIRECT_PARAM))).thenReturn("x");

    Assert.assertEquals(Authentication.SEND_CONTINUE, authenticator.redirectToLogin(req, res));
    redirect = ArgumentCaptor.forClass(String.class);
    Mockito.verify(res).sendRedirect(redirect.capture());
    Assert.assertEquals(
        "http://localhost:18631/security/login?" +
            SSOConstants.REQUESTED_URL_PARAM +
            "=" +
            URLEncoder.encode("http://foo/bar?a=A&b=B", "UTF-8") + "&" + SSOConstants.REPEATED_REDIRECT_PARAM + "=",
        redirect.getValue()
    );
  }

  @Test
  public void testRedirectToLoginUsingMetaRedirect() throws Exception {
    RemoteSSOService ssoService = new RemoteSSOService();
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "authToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "componentId");
    conf.set(SSOUserAuthenticator.HTTP_META_REDIRECT_TO_SSO, true);
    ssoService.setConfiguration(conf);
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator(ssoService, conf);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getQueryString()).thenReturn("a=A&b=B");

    StringWriter output = new StringWriter();
    PrintWriter printWriter = new PrintWriter(output);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(res.getWriter()).thenReturn(printWriter);

    Assert.assertEquals(Authentication.SEND_CONTINUE, authenticator.redirectToLogin(req, res));
    Mockito.verify(res).setContentType(Mockito.eq("text/html"));
    Mockito.verify(res).setStatus(Mockito.eq(HttpServletResponse.SC_OK));

    printWriter.flush();
    Assert.assertEquals(String.format(
        SSOUserAuthenticator.HTML_META_REDIRECT,
        authenticator.getLoginUrl(req, false, null, null)),
        output.toString().trim()
    );
  }

  @Test
  public void testRedirectToLogout() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    Mockito.when(ssoService.getLogoutUrl()).thenReturn("http://foo/logout");
    SSOUserAuthenticator authenticator = new SSOUserAuthenticator(ssoService, conf);

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Authentication auth = authenticator.redirectToLogout(res);
    Assert.assertEquals(Authentication.SEND_SUCCESS, auth);
    Mockito.verify(res).sendRedirect(Mockito.eq(ssoService.getLogoutUrl()));
  }

  @Test
  public void testreturnUnauthorized() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    ssoService.setConfiguration(new Configuration());
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    Mockito
        .doReturn("http://foo")
        .when(authenticator)
        .getLoginUrl(Mockito.any(HttpServletRequest.class), Mockito.anyBoolean(), Mockito.any(), Mockito.any());

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getServerPort()).thenReturn(1000);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(res.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.returnUnauthorized(req, res, "principal", "template"));
    Mockito.verify(authenticator).redirectToLogin(Mockito.eq(req), Mockito.eq(res));
    ArgumentCaptor<Cookie> cookieCaptor = ArgumentCaptor.forClass(Cookie.class);
    Mockito.verify(authenticator, Mockito.times(1)).createAuthCookie(Mockito.eq(req), Mockito.eq(""), Mockito.eq(0L));
    Mockito.verify(res, Mockito.times(1)).addCookie(cookieCaptor.capture());
    Assert.assertEquals(authenticator.getAuthCookieName(req), cookieCaptor.getValue().getName());
    Assert.assertEquals("", cookieCaptor.getValue().getValue());
    Assert.assertEquals("/", cookieCaptor.getValue().getPath());
    Assert.assertEquals(0, cookieCaptor.getValue().getMaxAge());
  }

  @Test
  public void testreturnUnauthorizedREST() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    Mockito
        .doReturn("http://foo")
        .when(authenticator)
        .getLoginUrl(Mockito.any(HttpServletRequest.class), Mockito.anyBoolean(), Mockito.any(), Mockito.any());

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_REST_CALL))).thenReturn("foo");
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(res.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.returnUnauthorized(req, res, "principal", "template"));
    Mockito.verify(res).setContentType(Mockito.eq("application/json"));
  }

  @Test
  public void testCreateAuthCookie() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.doReturn("NAME").when(authenticator).getAuthCookieName(Mockito.eq(req));
    Mockito.when(req.isSecure()).thenReturn(true);

    // persistent session
    Cookie cookie = authenticator.createAuthCookie(req, "token", System.currentTimeMillis() + 1000000);
    Assert.assertEquals("NAME", cookie.getName());
    Assert.assertEquals("token", cookie.getValue());
    Assert.assertEquals("/", cookie.getPath());
    Assert.assertTrue(cookie.getMaxAge() > 0);
    Assert.assertEquals(req.isSecure(), cookie.getSecure());

    // transient session
    cookie = authenticator.createAuthCookie(req, "token", -(System.currentTimeMillis() + 1000000));
    Assert.assertEquals(-1, cookie.getMaxAge());
  }

  @Test
  public void testSetAuthCookieIfNecessary() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn("TOKEN").when(authenticator).getAuthTokenFromCookie(Mockito.eq(req));
    Mockito.doReturn(Mockito.mock(Cookie.class)).when(authenticator).createAuthCookie(Mockito.eq(req), Mockito.eq
        ("TOKEN"), Mockito.anyInt());
    authenticator.setAuthCookieIfNecessary(req, res, "TOKEN", 1);
    Mockito.verify(authenticator, Mockito.times(0)).createAuthCookie(Mockito.eq(req), Mockito.eq("TOKEN"), Mockito
        .anyInt());
    Mockito.verify(res, Mockito.times(0)).addCookie(Mockito.any(Cookie.class));

    authenticator.setAuthCookieIfNecessary(req, res, "TOKENX", 1);
    Mockito.verify(authenticator, Mockito.times(1)).createAuthCookie(Mockito.eq(req), Mockito.eq("TOKENX"), Mockito
        .anyInt());
    Mockito.verify(res, Mockito.times(1)).addCookie(Mockito.any(Cookie.class));
  }

  @Test
  public void testIsLogoutRequest() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    Mockito.when(req.getContextPath()).thenReturn("/foo");
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getRequestURI()).thenReturn("/foo/logout");
    Assert.assertTrue(authenticator.isLogoutRequest(req));

    Mockito.when(req.getContextPath()).thenReturn("/foo");
    Mockito.when(req.getMethod()).thenReturn("PST");
    Mockito.when(req.getRequestURI()).thenReturn("/foo/logout");
    Assert.assertFalse(authenticator.isLogoutRequest(req));

    Mockito.when(req.getContextPath()).thenReturn("/foo");
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getRequestURI()).thenReturn("/foo/x");
    Assert.assertFalse(authenticator.isLogoutRequest(req));
  }

  @Test
  public void testGetAuthCookieName() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    Mockito.when(req.getServerPort()).thenReturn(1000);
    Assert.assertEquals(SSOConstants.AUTHENTICATION_COOKIE_PREFIX + "1000", authenticator.getAuthCookieName(req));
  }

  @Test
  public void testGetAuthTokenFromCookie() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.doReturn("authCookie").when(authenticator).getAuthCookieName(Mockito.eq(req));

    Assert.assertNull(authenticator.getAuthTokenFromCookie(req));

    Cookie cookie = Mockito.mock(Cookie.class);
    Mockito.when(cookie.getName()).thenReturn("foo");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {cookie});
    Assert.assertNull(authenticator.getAuthTokenFromCookie(req));

    Cookie cookie1 = Mockito.mock(Cookie.class);
    Mockito.when(cookie1.getName()).thenReturn("foo");
    Cookie cookie2 = Mockito.mock(Cookie.class);
    Mockito.when(cookie2.getName()).thenReturn("authCookie");
    Mockito.when(cookie2.getValue()).thenReturn("VALUE");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {cookie1, cookie2, cookie});
    Assert.assertEquals("VALUE", authenticator.getAuthTokenFromCookie(req));
  }

  @Test
  public void testIsAuthTokenInQueryString() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    Assert.assertFalse(authenticator.isAuthTokenInQueryString(req));

    Mockito.when(req.getParameter(Mockito.eq(SSOConstants.USER_AUTH_TOKEN_PARAM))).thenReturn("foo");
    Assert.assertTrue(authenticator.isAuthTokenInQueryString(req));
  }

  @Test
  public void testGetAuthTokenFromRequest() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    Assert.assertNull(authenticator.getAuthTokenFromRequest(req));

    Mockito.doReturn("fromCookie").when(authenticator).getAuthTokenFromCookie(Mockito.eq(req));
    Assert.assertEquals("fromCookie", authenticator.getAuthTokenFromRequest(req));

    Mockito.doReturn("fromHeader").when(req).getHeader(Mockito.eq(SSOConstants.X_USER_AUTH_TOKEN));
    Assert.assertEquals("fromHeader", authenticator.getAuthTokenFromRequest(req));

    Mockito.doReturn("fromParameter").when(req).getParameter(Mockito.eq(SSOConstants.USER_AUTH_TOKEN_PARAM));
    Assert.assertEquals("fromParameter", authenticator.getAuthTokenFromRequest(req));
  }

  @Test
  public void testValidateRequestNotMandatory() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Assert.assertEquals(Authentication.NOT_CHECKED, authenticator.validateRequest(req, res, false));
  }

  @Test
  public void testValidateRequestMandatoryNoAuthToken() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(Authentication.SEND_FAILURE).when(authenticator).returnUnauthorized(Mockito.eq(req), Mockito.eq
        (res), Mockito.anyString(), Mockito.anyString());

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.validateRequest(req, res, true));
    Mockito
        .verify(authenticator)
        .returnUnauthorized(Mockito.eq(req), Mockito.eq(res), Mockito.anyString(), Mockito.anyString());
    Mockito.verifyNoMoreInteractions(ssoService);
  }

  @Test
  public void testValidateRequestMandatoryInvalidAuthToken() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn("token").when(authenticator).getAuthTokenFromRequest(Mockito.eq(req));

    Mockito.doReturn(Authentication.SEND_FAILURE).when(authenticator).returnUnauthorized(Mockito.eq(req), Mockito.eq
        (res), Mockito.anyString(), Mockito.anyString());

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.validateRequest(req, res, true));
    Mockito
        .verify(authenticator)
        .returnUnauthorized(Mockito.eq(req), Mockito.eq(res), Mockito.anyString(), Mockito.anyString());
    Mockito.verify(ssoService).validateUserToken(Mockito.eq("token"));
    Mockito.verifyNoMoreInteractions(ssoService);
  }

  @Test
  public void testValidateRequestMandatoryValidAuthToken() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn("token").when(authenticator).getAuthTokenFromRequest(Mockito.eq(req));
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);
    Mockito.when(principal.getTokenStr()).thenReturn("token");
    Mockito.when(principal.getExpires()).thenReturn(1L);
    Mockito.doReturn(principal).when(ssoService).validateUserToken(Mockito.eq("token"));

    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.eq(req));
    Mockito.doNothing().when(authenticator).setAuthCookieIfNecessary(Mockito.eq(req), Mockito.eq(res), Mockito.eq
        ("token"), Mockito.eq(1));
    Mockito.doReturn(false).when(authenticator).isAuthTokenInQueryString(Mockito.eq(req));

    Authentication auth = authenticator.validateRequest(req, res, true);
    Assert.assertNotNull(auth);
    Assert.assertSame(principal, ((SSOAuthenticationUser)auth).getSSOUserPrincipal());

    Mockito.verify(authenticator).isLogoutRequest(Mockito.eq(req));
    Mockito
        .verify(authenticator)
        .setAuthCookieIfNecessary(Mockito.eq(req), Mockito.eq(res), Mockito.eq("token"), Mockito.eq(1L));
    Mockito.verify(authenticator).isAuthTokenInQueryString(Mockito.eq(req));

    Mockito.verify(ssoService).validateUserToken(Mockito.eq("token"));
    Mockito.verifyNoMoreInteractions(ssoService);
  }

  @Test
  public void testValidateRequestMandatoryValidAuthTokenWithTokenInQueryString() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn("token").when(authenticator).getAuthTokenFromRequest(Mockito.eq(req));
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);
    Mockito.when(principal.getTokenStr()).thenReturn("token");
    Mockito.when(principal.getExpires()).thenReturn(1L);
    Mockito.doReturn(principal).when(ssoService).validateUserToken(Mockito.eq("token"));

    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.eq(req));
    Mockito.doNothing().when(authenticator).setAuthCookieIfNecessary(Mockito.eq(req), Mockito.eq(res), Mockito.eq
        ("token"), Mockito.eq(1));
    Mockito.doReturn(Authentication.SEND_CONTINUE).when(authenticator).redirectToSelf(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn(true).when(authenticator).isAuthTokenInQueryString(Mockito.eq(req));

    Authentication auth = authenticator.validateRequest(req, res, true);
    Assert.assertNotNull(auth);
    Assert.assertSame(Authentication.SEND_CONTINUE, auth);

    Mockito.verify(authenticator).isLogoutRequest(Mockito.eq(req));
    Mockito
        .verify(authenticator)
        .setAuthCookieIfNecessary(Mockito.eq(req), Mockito.eq(res), Mockito.eq("token"), Mockito.eq(1L));
    Mockito.verify(authenticator).isAuthTokenInQueryString(Mockito.eq(req));

    Mockito.verify(ssoService).validateUserToken(Mockito.eq("token"));
    Mockito.verifyNoMoreInteractions(ssoService);
    Mockito.verify(authenticator).redirectToSelf(Mockito.eq(req), Mockito.eq(res));
  }

  @Test
  public void testValidateRequestMandatoryValidAuthTokenLogoutRequest() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, conf));
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn("token").when(authenticator).getAuthTokenFromRequest(Mockito.eq(req));
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);
    Mockito.when(principal.getTokenStr()).thenReturn("token");
    Mockito.when(principal.getExpires()).thenReturn(1L);
    Mockito.doReturn(principal).when(ssoService).validateUserToken(Mockito.eq("token"));

    Mockito.doReturn(true).when(authenticator).isLogoutRequest(Mockito.eq(req));
    Mockito.doReturn(Authentication.SEND_SUCCESS).when(authenticator).redirectToLogout(Mockito.eq(res));

    Authentication auth = authenticator.validateRequest(req, res, true);
    Assert.assertNotNull(auth);
    Assert.assertSame(Authentication.SEND_SUCCESS, auth);

    Mockito.verify(authenticator).isLogoutRequest(Mockito.eq(req));
    Mockito
        .verify(authenticator, Mockito.times(0))
        .setAuthCookieIfNecessary(Mockito.eq(req), Mockito.eq(res), Mockito.eq("token"), Mockito.eq(1L));

    Mockito.verify(ssoService).validateUserToken(Mockito.eq("token"));
    Mockito.verify(ssoService, Mockito.times(1)).invalidateUserToken(Mockito.eq("token"));
  }

}
