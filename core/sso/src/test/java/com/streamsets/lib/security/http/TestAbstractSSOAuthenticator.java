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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

public class TestAbstractSSOAuthenticator {

  private static class ForTestSSOAuthenticator extends AbstractSSOAuthenticator {

    public ForTestSSOAuthenticator(SSOService ssoService) {
      super(ssoService);
    }

    @Override
    protected Logger getLog() {
      Logger logger = Mockito.mock(Logger.class);
      Mockito.when(logger.isDebugEnabled()).thenReturn(true);
      return logger;
    }

    @Override
    public Authentication validateRequest(
        ServletRequest request, ServletResponse response, boolean mandatory
    ) throws ServerAuthException {
      return null;
    }
  }

  @Test
  public void testConstructorAndBasicMethods() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    AbstractSSOAuthenticator authenticator = new ForTestSSOAuthenticator(ssoService);
    Assert.assertEquals(ssoService, authenticator.getSsoService());
    Assert.assertEquals(SSOConstants.AUTHENTICATION_METHOD, authenticator.getAuthMethod());
    Assert.assertTrue(authenticator.secureResponse(null, null, true, null));
    authenticator.prepareRequest(null);
  }

  @Test
  public void testGetRequestInfoForLogging() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    AbstractSSOAuthenticator authenticator = new ForTestSSOAuthenticator(ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("url"));
    Mockito.when(req.getRemoteAddr()).thenReturn("remoteAddress");
    Mockito.when(req.getMethod()).thenReturn("method");
    String got = authenticator.getRequestInfoForLogging(req, "principalId");
    Assert.assertTrue(got.contains("remoteAddress"));
    Assert.assertTrue(got.contains("principalId"));
    Assert.assertTrue(got.contains("method"));
    Assert.assertTrue(got.contains("url"));
    Assert.assertFalse(got.contains("<QUERY_STRING>"));

    Mockito.when(req.getQueryString()).thenReturn("QS");
    got = authenticator.getRequestInfoForLogging(req, "principalId");
    Assert.assertTrue(got.contains("remoteAddress"));
    Assert.assertTrue(got.contains("principalId"));
    Assert.assertTrue(got.contains("method"));
    Assert.assertTrue(got.contains("url"));
    Assert.assertTrue(got.contains("<QUERY_STRING>"));
  }

  @Test
  public void testReturnForbidden() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    AbstractSSOAuthenticator authenticator = new ForTestSSOAuthenticator(ssoService);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getRequestURL()).thenReturn(new StringBuffer("url"));
    Mockito.when(req.getRemoteAddr()).thenReturn("remoteAddress");
    Mockito.when(req.getMethod()).thenReturn("method");
    Mockito.when(req.getQueryString()).thenReturn("QS");

    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    Mockito.when(res.getWriter()).thenReturn(printWriter);

    Assert.assertEquals(Authentication.SEND_FAILURE, authenticator.returnUnauthorized(req, res, "principal", "template"));

    ArgumentCaptor<Integer> error = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(res).setStatus(error.capture());
    Assert.assertEquals(
        SSOUserAuthenticator.UNAUTHORIZED_JSON,
        new ObjectMapper().readValue(writer.toString().trim(), Map.class)
    );
    Mockito.verify(res).setContentType(Mockito.eq("application/json"));
  }

}
