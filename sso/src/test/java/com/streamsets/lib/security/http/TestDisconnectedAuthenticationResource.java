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
import javax.ws.rs.core.Response;

public class TestDisconnectedAuthenticationResource {

  @Test
  public void testResource() {
    AuthenticationResourceHandler handler = Mockito.mock(AuthenticationResourceHandler.class);
    Response response = Mockito.mock(Response.class);
    Mockito.when(handler.login(Mockito.any(HttpServletRequest.class), Mockito.any(LoginJson.class))).thenReturn(response);

    DisconnectedSSOService ssoService = Mockito.mock(DisconnectedSSOService.class);

    DisconnectedAuthenticationResource resource = new DisconnectedAuthenticationResource(handler, ssoService);

    // disabled
    Mockito.when(ssoService.isEnabled()).thenReturn(false);
    Response got = resource.login(null, null);
    Assert.assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), got.getStatus());

    //enabled
    Mockito.when(ssoService.isEnabled()).thenReturn(true);
    Assert.assertEquals(response, resource.login(null, null));
  }

}
