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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestDisconnectedSSOService {

  @Test
  public void testService() throws Exception {
    DisconnectedSessionHandler handler = Mockito.mock(DisconnectedSessionHandler.class);
    DisconnectedAuthentication authentication = Mockito.mock(DisconnectedAuthentication.class);
    Mockito.when(authentication.getSessionHandler()).thenReturn(handler);
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);
    Mockito.when(handler.get(Mockito.eq("ut"))).thenReturn(principal);
    DisconnectedSSOService service = new DisconnectedSSOService(authentication);
    service.setConfiguration(new Configuration());
    Assert.assertEquals("/security/login", service.getLoginPageUrl());
    Assert.assertEquals("/security/_logout", service.getLogoutUrl());
    Assert.assertFalse(service.isEnabled());
    service.setEnabled(true);
    Assert.assertTrue(service.isEnabled());
    service.register(null); //NOP
    Assert.assertEquals(principal, service.validateUserTokenWithSecurityService("ut"));
    try {
      service.validateAppTokenWithSecurityService("at", "ci");
    } catch (UnsupportedOperationException ex) {
      //OK
    }
  }
}
