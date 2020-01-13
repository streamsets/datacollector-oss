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

public class TestSSOUtils {

  @Test
  public void testGetAuthTokenForLogging() {
    SSOService ssoService = Mockito.mock(SSOService.class);
    SSOUserAuthenticator authenticator = Mockito.spy(new SSOUserAuthenticator(ssoService, new Configuration(), null));
    Assert.assertEquals("TOKEN:null", SSOUtils.tokenForLog(null));
    Assert.assertEquals("TOKEN:abcdefghij123456...", SSOUtils.tokenForLog("abcdefghij1234567"));
  }


}
