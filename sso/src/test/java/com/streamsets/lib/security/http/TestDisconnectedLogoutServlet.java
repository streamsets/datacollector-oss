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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class TestDisconnectedLogoutServlet {

  @Test
  public void testServlet() throws Exception {
    DisconnectedSSOService service = Mockito.mock(DisconnectedSSOService.class);
    DisconnectedLogoutServlet servlet = new DisconnectedLogoutServlet(service) {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        res.setStatus(HttpServletResponse.SC_OK);
      }
    };

    servlet = Mockito.spy(servlet);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    // service disabled
    Mockito.when(service.isEnabled()).thenReturn(false);
    servlet.service(req, res);
    Mockito.verify(res, Mockito.times(1)).sendError(Mockito.eq(HttpServletResponse.SC_SERVICE_UNAVAILABLE));

    // service enabled
    Mockito.reset(res);
    Mockito.when(service.isEnabled()).thenReturn(true);
    servlet.service(req, res);
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_OK));
  }

}
