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

public class TestSSOPrincipalUtils {

  @Test
  public void getGetClientIpAddress() {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    // no X-Forwarded-For header, no remote address expect unknown IP
    Assert.assertEquals(SSOPrincipalUtils.UNKNOWN_IP, SSOPrincipalUtils.getClientIpAddress(req));

    // no X-Forwarded-For header, remote address use remote address
    Mockito.when(req.getRemoteAddr()).thenReturn("remoteAddr");
    Assert.assertEquals("remoteAddr", SSOPrincipalUtils.getClientIpAddress(req));

    // X-Forwarded-For header unknown, remote address, return remote address
    // not sure when this would ever occur though, as "unknown" is not a standardized value for X-Forwarded-For
    Mockito.when(req.getHeader(Mockito.eq(SSOPrincipalUtils.CLIENT_IP_HEADER))).thenReturn(SSOPrincipalUtils.UNKNOWN_IP);
    Assert.assertEquals("remoteAddr", SSOPrincipalUtils.getClientIpAddress(req));

    // X-Forwarded-For header with single address, remote address
    Mockito.when(req.getHeader(Mockito.eq(SSOPrincipalUtils.CLIENT_IP_HEADER))).thenReturn("clientIpAddr");
    Assert.assertEquals("clientIpAddr", SSOPrincipalUtils.getClientIpAddress(req));

    // X-Forwarded-For header with multiple addresses, remote address
    Mockito.when(req.getHeader(Mockito.eq(SSOPrincipalUtils.CLIENT_IP_HEADER))).thenReturn("clientIpAddr, proxy1,proxy2");
    Assert.assertEquals("clientIpAddr", SSOPrincipalUtils.getClientIpAddress(req));
  }

  @Test
  public void testSetRequestInfo() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getHeader(Mockito.eq(SSOPrincipalUtils.CLIENT_IP_HEADER))).thenReturn("foo");
    SSOPrincipal principal = new SSOPrincipalJson();
    SSOPrincipalUtils.setRequestInfo(principal, request);
    Assert.assertEquals("foo", principal.getRequestIpAddress());
  }

}
