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

import com.streamsets.datacollector.util.Configuration;
import org.eclipse.jetty.server.Authentication;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import static org.mockito.Mockito.doReturn;

public class TestSSOAuthenticator {

  @Test
  public void testRequestIpAddress() throws Exception {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOAuthenticator authenticator = new SSOAuthenticator(null, ssoService, new Configuration(), null);
    authenticator = Mockito.spy(authenticator);

    // faking delegation to real authenticator to return a non-authenticated Authentication
    doReturn(Authentication.NOT_CHECKED)
        .when(authenticator)
        .validateRequestDelegation(Mockito.any(ServletRequest.class),
            Mockito.any(ServletResponse.class),
            Mockito.anyBoolean()
        );

    // making threadlocal dirty
    SSOPrincipalJson principal = new SSOPrincipalJson();
    principal.setRequestIpAddress("foo");

    // verifying request IP is set to dirty value
    principal = new SSOPrincipalJson();
    Assert.assertEquals("foo", principal.getRequestIpAddress());

    //request returns 'bar' as remote address
    ServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn("bar");

    Assert.assertEquals(Authentication.NOT_CHECKED, authenticator.validateRequest(request, null, true));

    // verifying request IP has been reset because non-authenticated Authentication
    principal = new SSOPrincipalJson();
    Assert.assertNull(principal.getRequestIpAddress());

    // faking delegation to real authenticator to return an authenticated Authentication
    SSOPrincipalJson principalJson = new SSOPrincipalJson();
    principalJson.setTokenStr("token");
    SSOAuthenticationUser authentication = new SSOAuthenticationUser(principalJson);
    Mockito
        .doReturn(authentication)
        .when(authenticator)
        .validateRequestDelegation(Mockito.any(ServletRequest.class),
            Mockito.any(ServletResponse.class),
            Mockito.anyBoolean()
        );

    // making threadlocal dirty
    principal = new SSOPrincipalJson();
    principal.setRequestIpAddress("foo");

    // verifying request IP is set to dirty value
    principal = new SSOPrincipalJson();
    Assert.assertEquals("foo", principal.getRequestIpAddress());

    //request returns 'bar' as remote address
    request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn("bar");

    Assert.assertEquals(authentication, authenticator.validateRequest(request, null, true));

    // the principal has the request IP
    Assert.assertEquals("bar", authentication.getSSOUserPrincipal().getRequestIpAddress());
  }
}
