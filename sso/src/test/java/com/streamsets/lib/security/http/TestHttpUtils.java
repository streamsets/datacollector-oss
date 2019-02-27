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

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

public class TestHttpUtils {

  @Test
  public void testGetLoginCookie() {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    // no cookies
    Assert.assertNull(HttpUtils.getLoginCookie(req));

    // no login cookie
    Cookie cookie = new Cookie("c", "v");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {cookie} );
    Assert.assertNull(HttpUtils.getLoginCookie(req));

    // login cookie
    Cookie loginCookie = new Cookie(HttpUtils.getLoginCookieName(), "v");
    Mockito.when(req.getCookies()).thenReturn(new Cookie[] {cookie, loginCookie} );
    Assert.assertEquals(loginCookie, HttpUtils.getLoginCookie(req));

  }

}
