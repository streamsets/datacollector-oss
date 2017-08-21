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
  public void testGetClientIpAddress() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

    Assert.assertEquals(SSOPrincipalUtils.UNKNOWN_IP, SSOPrincipalUtils.getClientIpAddress(request));

    Mockito.when(request.getRemoteAddr()).thenReturn("");
    Assert.assertEquals(SSOPrincipalUtils.UNKNOWN_IP, SSOPrincipalUtils.getClientIpAddress(request));

    Mockito.when(request.getRemoteAddr()).thenReturn("getRemoteAddr");
    Assert.assertEquals("getRemoteAddr", SSOPrincipalUtils.getClientIpAddress(request));

    Mockito.when(request.getHeader(Mockito.eq(SSOPrincipalUtils.CLIENT_IP_HEADER))).thenReturn("");
    Assert.assertEquals("getRemoteAddr", SSOPrincipalUtils.getClientIpAddress(request));

    Mockito.when(request.getHeader(Mockito.eq(SSOPrincipalUtils.CLIENT_IP_HEADER))).thenReturn("clientIpAddr");
    Assert.assertEquals("clientIpAddr", SSOPrincipalUtils.getClientIpAddress(request));
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
