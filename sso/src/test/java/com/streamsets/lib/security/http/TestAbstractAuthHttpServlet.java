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

import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class TestAbstractAuthHttpServlet {

  @Test
  public void testServlet() throws Exception {
    HttpServlet servlet = new AbstractAuthHttpServlet() {
      @Override
      protected SSOService getSsoService() {
        return null;
      }
    };

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    Mockito.when(req.getMethod()).thenReturn("HEAD");
    servlet.service(req, res);
    Mockito.verify(res, Mockito.times(1)).sendError(Mockito.eq(HttpServletResponse.SC_METHOD_NOT_ALLOWED));

    Mockito.when(req.getMethod()).thenReturn("OPTIONS");
    Mockito.reset(res);
    servlet.service(req, res);
    Mockito.verify(res, Mockito.times(1)).sendError(Mockito.eq(HttpServletResponse.SC_METHOD_NOT_ALLOWED));

    Mockito.when(req.getMethod()).thenReturn("PUT");
    Mockito.reset(res);
    servlet.service(req, res);
    Mockito.verify(res, Mockito.times(1)).sendError(Mockito.eq(HttpServletResponse.SC_METHOD_NOT_ALLOWED));

    Mockito.when(req.getMethod()).thenReturn("POST");
    Mockito.reset(res);
    servlet.service(req, res);
    Mockito.verify(res, Mockito.times(1)).sendError(Mockito.eq(HttpServletResponse.SC_METHOD_NOT_ALLOWED));

    Mockito.when(req.getMethod()).thenReturn("DELETE");
    Mockito.reset(res);
    servlet.service(req, res);
    Mockito.verify(res, Mockito.times(1)).sendError(Mockito.eq(HttpServletResponse.SC_METHOD_NOT_ALLOWED));

    Mockito.when(req.getMethod()).thenReturn("TRACE");
    Mockito.reset(res);
    Mockito.when(req.getHeader(Mockito.eq(SSOConstants.X_REST_CALL))).thenReturn("rest-call");
    servlet.service(req, res);
    Mockito.verify(res, Mockito.times(1)).sendError(Mockito.eq(HttpServletResponse.SC_METHOD_NOT_ALLOWED));
  }
}
