/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.SlaveRuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.CORSConstants;
import com.streamsets.lib.security.http.SSOAuthenticationUser;
import com.streamsets.lib.security.http.SSOPrincipal;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TestAsterAuthenticator {

  @Test
  public void testBasicMethods() throws Exception {
    AsterServiceImpl service = Mockito.mock(AsterServiceImpl.class);
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        new Configuration()
    );
    Mockito.when(service.getConfig()).thenReturn(config);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterAuthenticator authenticator = new AsterAuthenticator(service, runtimeInfo);

    // getRequestBaseUrl no URL path
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo"));
    Mockito.when(req.getRequestURI()).thenReturn("");
    Assert.assertEquals("http://foo", authenticator.getRequestBaseUrl(req));

    // getRequestBaseUrl with URL path
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getRequestURI()).thenReturn("/bar");
    Assert.assertEquals("http://foo", authenticator.getRequestBaseUrl(req));

    // getUrlForRedirection GET no query string
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getRequestURI()).thenReturn("/bar");
    Assert.assertEquals("http://foo/bar", authenticator.getUrlForRedirection(req));

    // getUrlForRedirection GET query string
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getQueryString()).thenReturn("a=A");
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getRequestURI()).thenReturn("/bar");
    Assert.assertEquals("http://foo/bar?a=A", authenticator.getUrlForRedirection(req));

    // getUrlForRedirection not GET
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("POST");
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("http://foo/bar"));
    Mockito.when(req.getRequestURI()).thenReturn("/bar");
    Assert.assertEquals("http://foo", authenticator.getUrlForRedirection(req));

    // addParameterToQueryString no query string
    Assert.assertEquals("http://foo?a=A", authenticator.addParameterToQueryString("http://foo", "a", "A"));

    // addParameterToQueryString query string
    Assert.assertEquals("http://foo?a=A&b=B", authenticator.addParameterToQueryString("http://foo?a=A", "b", "B"));

    // redirect
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    authenticator.redirect(null, res, "url");
    Mockito.verify(res, Mockito.times(1)).sendRedirect(Mockito.eq("url"));
    Mockito.verify(res, Mockito.times(1)).setHeader(Mockito.eq("Cache-Control"), Mockito.eq("no-store"));

    // isEngineRegistrationOrLoginPage no
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURI()).thenReturn("/bar");
    Assert.assertFalse(authenticator.isEngineRegistrationOrLoginPage(req));

    // isEngineRegistrationOrLoginPage yes
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURI()).thenReturn("/alogin.html");
    Assert.assertTrue(authenticator.isEngineRegistrationOrLoginPage(req));
    Mockito.when(req.getRequestURI()).thenReturn("/aregistration.html");
    Assert.assertTrue(authenticator.isEngineRegistrationOrLoginPage(req));

    // isValidState, no state in request
    req = Mockito.mock(HttpServletRequest.class);
    Assert.assertFalse(authenticator.isValidState(req));

    // isValidState, invalid state in request
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getParameter(Mockito.eq("lstate"))).thenReturn("invalid");
    Assert.assertFalse(authenticator.isValidState(req));

    // isValidState, valid state in request
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(service.isValidLocalState(Mockito.eq("valid"))).thenReturn(true);
    Mockito.when(req.getParameter(Mockito.eq("lstate"))).thenReturn("valid");
    Assert.assertTrue(authenticator.isValidState(req));

    // isHandlingEngineRegistration no
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURI()).thenReturn("/rest/v1/foo");
    Assert.assertFalse(authenticator.isHandlingEngineRegistration(req, res));

    // isHandlingEngineRegistration yes
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURI()).thenReturn("/rest/v1/aregistration");
    Assert.assertTrue(authenticator.isHandlingEngineRegistration(req, res));

    // iHandlingUserLogin no
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURI()).thenReturn("/rest/v1/foo");
    Assert.assertFalse(authenticator.isHandlingUserLogin(req, res));

    // iHandlingUserLogin yes
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURI()).thenReturn("/rest/v1/alogin");
    Assert.assertTrue(authenticator.isHandlingUserLogin(req, res));

    // shouldRetryFailedRequest yes
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Assert.assertTrue(authenticator.shouldRetryFailedRequest(req));

    // shouldRetryFailedRequest no
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getParameter(Mockito.eq("a_retry"))).thenReturn("false");
    Assert.assertFalse(authenticator.shouldRetryFailedRequest(req));

    // shouldRetryFailedRequest no
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("POST");
    Assert.assertFalse(authenticator.shouldRetryFailedRequest(req));

    // createPrincipal
    AsterUser user = new AsterUser().setName("foo@foo")
        .setOrg("org")
        .setRoles(ImmutableSet.of("engine:admin"))
        .setGroups(ImmutableSet.of("all"));
    SSOPrincipal principal = authenticator.createPrincipal(user);
    Assert.assertNotNull(principal);
    Assert.assertEquals("foo@foo", principal.getName());
    Assert.assertEquals("foo@foo", principal.getEmail());
    Assert.assertEquals("org", principal.getOrganizationId());
    Assert.assertEquals(ImmutableSet.of("admin", "user"), principal.getRoles());
    Assert.assertEquals(ImmutableSet.of("all"), principal.getGroups());
    long shouldExpire = System.currentTimeMillis() + 86400 * 1000;
    Assert.assertTrue(Math.abs( shouldExpire - principal.getExpires()) < 1000);

    // getSessionAuthentication no session
    req = Mockito.mock(HttpServletRequest.class);
    Assert.assertNull(authenticator.getSessionAuthentication(req));

    // getSessionAuthentication session no auth
    req = Mockito.mock(HttpServletRequest.class);
    HttpSession session = Mockito.mock(HttpSession.class);
    Mockito.when(req.getSession(Mockito.eq(false))).thenReturn(session);
    Assert.assertNull(authenticator.getSessionAuthentication(req));

    // getSessionAuthentication session with auth
    req = Mockito.mock(HttpServletRequest.class);
    session = Mockito.mock(HttpSession.class);
    Authentication sessionAuth = Mockito.mock(Authentication.class);
    Mockito.when(session.getAttribute(Mockito.eq(SessionAuthentication.__J_AUTHENTICATED))).thenReturn(sessionAuth);
    Mockito.when(req.getSession(Mockito.eq(false))).thenReturn(session);
    Assert.assertEquals(sessionAuth, authenticator.getSessionAuthentication(req));

    // authenticateSession
    req = Mockito.mock(HttpServletRequest.class);
    session = Mockito.mock(HttpSession.class);
    Mockito.when(req.getSession(Mockito.eq(true))).thenReturn(session);
    user = new AsterUser().setName("foo@foo")
        .setOrg("org")
        .setRoles(ImmutableSet.of("engine:admin"))
        .setGroups(ImmutableSet.of("all"));
    authenticator.authenticateSession(req, user);
    ArgumentCaptor<Authentication> authenticationArgumentCaptor = ArgumentCaptor.forClass(Authentication.class);
    Mockito.verify(session, Mockito.times(1)).setAttribute(
        Mockito.eq(SessionAuthentication.__J_AUTHENTICATED),
        authenticationArgumentCaptor.capture()
    );
    Assert.assertEquals(
        "foo@foo",
        ((SSOAuthenticationUser) authenticationArgumentCaptor.getValue()).getSSOUserPrincipal().getEmail()
    );

    // destroySession
    req = Mockito.mock(HttpServletRequest.class);
    session = Mockito.mock(HttpSession.class);
    Mockito.when(req.getSession(Mockito.eq(false))).thenReturn(session);
    authenticator.destroySession(req);
    Mockito.verify(session, Mockito.times(1)).invalidate();

    // isLogoutRequest no
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getContextPath()).thenReturn("");
    Mockito.when(req.getMethod()).thenReturn("POST");
    Mockito.when(req.getRequestURI()).thenReturn("/rest/v1/foo");
    Assert.assertFalse(authenticator.isLogoutRequest(req));

    // isLogoutRequest yes
    req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getContextPath()).thenReturn("");
    Mockito.when(req.getMethod()).thenReturn("POST");
    Mockito.when(req.getRequestURI()).thenReturn("/rest/v1/authentication/logout");
    Assert.assertTrue(authenticator.isLogoutRequest(req));

  }

  @Test
  public void testValidateRequestCors() throws Exception {
    AsterServiceImpl service = Mockito.mock(AsterServiceImpl.class);
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        new Configuration()
    );
    Mockito.when(service.getConfig()).thenReturn(config);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterAuthenticator authenticator = new AsterAuthenticator(service, runtimeInfo);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Mockito.when(req.getMethod()).thenReturn("OPTIONS");

    Assert.assertEquals(Authentication.SEND_SUCCESS, authenticator.validateRequest(req, res, true));

    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_OK));
    Mockito.verify(res, Mockito.times(1)).setHeader(Mockito.eq("Access-Control-Allow-Origin"), Mockito.eq(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT));
    Mockito.verify(res, Mockito.times(1)).setHeader(Mockito.eq("Access-Control-Allow-Headers"), Mockito.eq(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_HEADERS_DEFAULT));
    Mockito.verify(res, Mockito.times(1)).setHeader(Mockito.eq("Access-Control-Allow-Methods"), Mockito.eq(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_METHODS_DEFAULT));
  }

  @Test
  public void testValidateRequestNotMandatory() throws Exception {
    AsterServiceImpl service = Mockito.mock(AsterServiceImpl.class);
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        new Configuration()
    );
    Mockito.when(service.getConfig()).thenReturn(config);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterAuthenticator authenticator = new AsterAuthenticator(service, runtimeInfo);
    authenticator = Mockito.spy(authenticator);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Mockito.when(req.getMethod()).thenReturn("GET");

    // not mandatory
    Assert.assertEquals(Authentication.NOT_CHECKED, authenticator.validateRequest(req, res, false));

    // registration or login page and valid state
    Mockito.doReturn(true).when(authenticator).isEngineRegistrationOrLoginPage(Mockito.eq(req));
    Mockito.doReturn(true).when(authenticator).isValidState(Mockito.eq(req));
    Assert.assertEquals(Authentication.NOT_CHECKED, authenticator.validateRequest(req, res, false));
  }

  @Test
  public void testValidateRequestRegisteredEngine() throws Exception {
    AsterServiceImpl service = Mockito.mock(AsterServiceImpl.class);
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        new Configuration()
    );
    Mockito.when(service.getConfig()).thenReturn(config);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterAuthenticator authenticator = new AsterAuthenticator(service, runtimeInfo);
    authenticator = Mockito.spy(authenticator);

    Mockito.doReturn(true).when(authenticator).isEngineRegistered();

    // authenticated session not a logout request
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);
    Mockito.when(principal.getName()).thenReturn("foo@foo");
    SSOAuthenticationUser authentication = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(authentication.getSSOUserPrincipal()).thenReturn(principal);
    Mockito.doReturn(authentication).when(authenticator).getSessionAuthentication(Mockito.eq(req));
    Mockito.doReturn(false).when(authenticator).isLogoutRequest(Mockito.eq(req));

    Assert.assertEquals(authentication, authenticator.validateRequest(req, res, true));

    // not an authenticated session handling user login true GET initiate
    req = Mockito.mock(HttpServletRequest.class);
    res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(null).when(authenticator).getSessionAuthentication(Mockito.eq(req));

    Mockito.doReturn(true).when(authenticator).isHandlingUserLogin(Mockito.eq(req), Mockito.eq(res));

    Mockito.doReturn("http://foo").when(authenticator).getRequestBaseUrl(Mockito.eq(req));

    Mockito.when(service.handleUserLogin(Mockito.eq("http://foo"), Mockito.eq(req), Mockito.eq(res))).thenReturn(null);
    Assert.assertEquals(Authentication.SEND_SUCCESS, authenticator.validateRequest(req, res, true));

    // not an authenticated session handling user login true POST complete
    req = Mockito.mock(HttpServletRequest.class);
    res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(null).when(authenticator).getSessionAuthentication(Mockito.eq(req));

    Mockito.doReturn(true).when(authenticator).isHandlingUserLogin(Mockito.eq(req), Mockito.eq(res));

    Mockito.doReturn("http://foo").when(authenticator).getRequestBaseUrl(Mockito.eq(req));

    AsterUser user = Mockito.mock(AsterUser.class);
    Mockito.when(user.getPreLoginUrl()).thenReturn("http://foo");
    Mockito.when(service.handleUserLogin(Mockito.eq("http://foo"), Mockito.eq(req), Mockito.eq(res))).thenReturn(user);

    Mockito.doNothing().when(authenticator).authenticateSession(Mockito.eq(req), Mockito.eq(user));
    Assert.assertEquals(Authentication.SEND_SUCCESS, authenticator.validateRequest(req, res, true));
    Mockito.verify(authenticator, Mockito.times(1)).authenticateSession(Mockito.eq(req), Mockito.eq(user));
    Mockito.verify(authenticator, Mockito.times(1)).redirect(
        Mockito.eq(req),
        Mockito.eq(res),
        Mockito.eq("http://foo")
    );

    // not an authenticated session not handling user login
    req = Mockito.mock(HttpServletRequest.class);
    res = Mockito.mock(HttpServletResponse.class);
    Mockito.doReturn(null).when(authenticator).getSessionAuthentication(Mockito.eq(req));

    Mockito.doReturn(false).when(authenticator).isHandlingUserLogin(Mockito.eq(req), Mockito.eq(res));

    Mockito.doNothing().when(authenticator).destroySession(Mockito.eq(req));

    Mockito.doReturn("http://foo").when(authenticator).getUrlForRedirection(Mockito.eq(req));

    Mockito.when(service.storeRedirUrl(Mockito.eq("http://foo"))).thenReturn("redirKey");

    Assert.assertEquals(Authentication.SEND_SUCCESS, authenticator.validateRequest(req, res, true));
    ArgumentCaptor<String> redirectCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(res, Mockito.times(1)).sendRedirect(redirectCaptor.capture());
    Assert.assertEquals("/alogin.html?lstate=redirKey", redirectCaptor.getValue());

  }

  @Test
  public void testValidateRequestNotRegisteredEngine() throws Exception {
    AsterServiceImpl service = Mockito.mock(AsterServiceImpl.class);
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        new Configuration()
    );
    Mockito.when(service.getConfig()).thenReturn(config);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterAuthenticator authenticator = new AsterAuthenticator(service, runtimeInfo);
    authenticator = Mockito.spy(authenticator);

    Mockito.doReturn(false).when(authenticator).isEngineRegistered();

    // handling registration initiate
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Mockito.doReturn(true).when(authenticator).isHandlingEngineRegistration(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn("http://foo").when(authenticator).getRequestBaseUrl(Mockito.eq(req));

    Assert.assertEquals(Authentication.SEND_SUCCESS, authenticator.validateRequest(req, res, true));
    Mockito.when(service.handleUserLogin(Mockito.eq("http://foo"), Mockito.eq(req), Mockito.eq(res))).thenReturn(null);

    // handling registration complete

    req = Mockito.mock(HttpServletRequest.class);
    res = Mockito.mock(HttpServletResponse.class);

    Mockito.doReturn(true).when(authenticator).isHandlingEngineRegistration(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn("http://foo").when(authenticator).getRequestBaseUrl(Mockito.eq(req));
    Mockito.when(service.handleEngineRegistration(Mockito.eq("http://foo"), Mockito.eq(req), Mockito.eq(res))).thenReturn("redir");

    Authentication authentication = Mockito.mock(Authentication.class);
    Mockito.doReturn(authentication).when(authenticator).redirect(Mockito.eq(req), Mockito.eq(res), Mockito.eq("redir"));

    Assert.assertEquals(authentication, authenticator.validateRequest(req, res, true));
  }

  @Test
  public void testValidateExceptions() throws Exception {
    AsterServiceImpl service = Mockito.mock(AsterServiceImpl.class);
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        new Configuration()
    );
    Mockito.when(service.getConfig()).thenReturn(config);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterAuthenticator authenticator = new AsterAuthenticator(service, runtimeInfo);
    authenticator = Mockito.spy(authenticator);

    // first authenticator method call within try/catch block. AsterException
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.doReturn("http://foo").when(authenticator).getUrlForRedirection(Mockito.eq(req));
    Mockito.doReturn(false).when(authenticator).shouldRetryFailedRequest(Mockito.eq(req));

    Mockito.doThrow(new AsterException("")).when(authenticator).isEngineRegistrationOrLoginPage(Mockito.eq(req));

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.validateRequest(req, res, true));
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_BAD_REQUEST));

    // first authenticator method call within try/catch block. AsterAuthException
    req = Mockito.mock(HttpServletRequest.class);
    res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.doReturn("http://foo").when(authenticator).getUrlForRedirection(Mockito.eq(req));
    Mockito.doReturn(false).when(authenticator).shouldRetryFailedRequest(Mockito.eq(req));

    Mockito.doThrow(new AsterAuthException("")).when(authenticator).isEngineRegistrationOrLoginPage(Mockito.eq(req));

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.validateRequest(req, res, true));
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_UNAUTHORIZED));

    // first authenticator method call within try/catch block. Exception
    req = Mockito.mock(HttpServletRequest.class);
    res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.doReturn("http://foo").when(authenticator).getUrlForRedirection(Mockito.eq(req));
    Mockito.doReturn(false).when(authenticator).shouldRetryFailedRequest(Mockito.eq(req));

    Mockito.doThrow(new RuntimeException("")).when(authenticator).isEngineRegistrationOrLoginPage(Mockito.eq(req));

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.validateRequest(req, res, true));
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR));

    // should retry redirection
    Mockito.doReturn(true).when(authenticator).shouldRetryFailedRequest(Mockito.eq(req));

    req = Mockito.mock(HttpServletRequest.class);
    res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.doReturn("http://foo").when(authenticator).getUrlForRedirection(Mockito.eq(req));

    Mockito.doThrow(new AsterException("")).when(authenticator).isEngineRegistrationOrLoginPage(Mockito.eq(req));

    Authentication redirAuth = Mockito.mock(Authentication.class);
    Mockito.doReturn(redirAuth).when(authenticator).redirect(
        Mockito.eq(req),
        Mockito.eq(res),
        Mockito.eq("http://foo?a_retry=false")
    );

    Assert.assertEquals(redirAuth, authenticator.validateRequest(req, res, true));
  }

  @Test
  public void testLogout() throws Exception {
    AsterServiceImpl service = Mockito.mock(AsterServiceImpl.class);
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        new Configuration()
    );
    Mockito.when(service.getConfig()).thenReturn(config);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterAuthenticator authenticator = new AsterAuthenticator(service, runtimeInfo);
    authenticator = Mockito.spy(authenticator);

    Mockito.doReturn(true).when(authenticator).isEngineRegistered();

    // handling registration initiate
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURI()).thenReturn("/rest/v1/logout");
    Mockito.when(req.getMethod()).thenReturn("POST");
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);


    req = Mockito.mock(HttpServletRequest.class);
    res = Mockito.mock(HttpServletResponse.class);

    Mockito.doReturn(true).when(authenticator).isHandlingEngineRegistration(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn(true).when(authenticator).isLogoutRequest(Mockito.eq(req));
    SSOAuthenticationUser authentication = Mockito.mock(SSOAuthenticationUser.class);
    Mockito.when(authentication.getSSOUserPrincipal()).thenReturn(Mockito.mock(SSOPrincipal.class));
    Mockito.doReturn(authentication).when(authenticator).getSessionAuthentication(Mockito.eq(req));

    Assert.assertEquals(Authentication.SEND_SUCCESS, authenticator.validateRequest(req, res, true));
    Mockito.verify(authenticator, Mockito.times(1)).destroySession(Mockito.eq(req));
    Mockito.verify(service, Mockito.times(1)).handleLogout(Mockito.eq(req), Mockito.eq(res));
  }

  @Test
  public void testClusterSlave() throws Exception {
    AsterServiceImpl service = Mockito.mock(AsterServiceImpl.class);
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        new Configuration()
    );
    Mockito.when(service.getConfig()).thenReturn(config);
    RuntimeInfo runtimeInfo = Mockito.mock(SlaveRuntimeInfo.class);
    Mockito.when(runtimeInfo.isClusterSlave()).thenReturn(true);
    AsterAuthenticator authenticator = Mockito.spy(new AsterAuthenticator(service, runtimeInfo));

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);


    // Make sure cluster slave auth mechanism is called.
    Assert.assertNull(authenticator.validateRequest(req, res, true));
    Mockito.verify(authenticator).handleAuthForClusterSlave(Mockito.eq(req));


    String validMockToken = "valid|admin";
    String inValidMockToken = "invalid|admin";

    Mockito.when(runtimeInfo.getAuthenticationTokens()).thenReturn(Collections.singletonMap("admin", validMockToken));
    Mockito.when(runtimeInfo.isValidAuthenticationToken(Mockito.eq(validMockToken))).thenReturn(true);

    // Now invalid auth token
    Mockito.when(req.getParameter(Mockito.eq("auth_token"))).thenReturn(inValidMockToken);
    Mockito.when(req.getParameter(Mockito.eq("auth_user"))).thenReturn("admin");
    Assert.assertNull(authenticator.validateRequest(req, res, true));

    Map<String, Object> sessionMap = new HashMap<>();

    HttpSession httpSession = Mockito.mock(HttpSession.class);
    Mockito.when(req.getSession(Mockito.anyBoolean())).thenReturn(httpSession);
    Mockito.doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      sessionMap.put((String)args[0], args[1]);
      return null;
    }).when(httpSession).setAttribute(Mockito.anyString(), Mockito.anyString());

    Mockito.doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      return sessionMap.get(args[0].toString());
    }).when(httpSession).getAttribute(Mockito.anyString());

    Mockito.when(runtimeInfo.getRolesFromAuthenticationToken(Mockito.eq(validMockToken))).thenReturn(new String[]{"user", "admin"});

    //Now with valid token
    Mockito.when(req.getParameter(Mockito.eq("auth_token"))).thenReturn(validMockToken);
    Mockito.when(req.getParameter(Mockito.eq("auth_user"))).thenReturn("admin");
    Authentication auth = authenticator.validateRequest(req, res, true);
    Assert.assertNotNull(auth);

    Assert.assertTrue(auth instanceof SSOAuthenticationUser);
    SSOAuthenticationUser user = (SSOAuthenticationUser)auth;
    Assert.assertNotNull(user.getSSOUserPrincipal());
    Assert.assertEquals("admin", user.getSSOUserPrincipal().getName());
    Assert.assertEquals(2, user.getSSOUserPrincipal().getRoles().size());
    Assert.assertEquals(ImmutableSet.of("user", "admin"), user.getSSOUserPrincipal().getRoles());


  }
}
