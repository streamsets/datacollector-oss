/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.util.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.UUID;

public class TestAsterServiceImpl {

  private AsterServiceHook hook;

  private AsterServiceImpl createService(File file) {
    Configuration innerConfig = new Configuration();
    innerConfig.set(AsterServiceProvider.ASTER_URL, "http://dummy-aster-url:1234");
    return createService(file, innerConfig);
  }

  private AsterServiceImpl createService(File file, Configuration innerConfig) {
    AsterServiceConfig config = new AsterServiceConfig(
        AsterRestConfig.SubjectType.DC,
        "1",
        UUID.randomUUID().toString(),
        innerConfig
    );

    AsterServiceImpl asterService = new AsterServiceImpl(config, file);
    hook = Mockito.mock(AsterServiceHook.class);
    asterService.registerHooks(Collections.singletonList(hook));
    return asterService;
  }

  @Test
  public void testServiceBasicMethods() throws IOException {
    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterServiceImpl service = createService(file);

    // config
    Assert.assertNotNull(service.getConfig());

    // rest client
    AsterRestClientImpl asterRest = service.getRestClient();
    Assert.assertNotNull(asterRest);
    Assert.assertEquals(service.getConfig().getAsterRestConfig(), asterRest.getConfig());

    // tokens file does not exist
    Assert.assertFalse(service.isEngineRegistered());

    // tokens file exists
    try (OutputStream os = new FileOutputStream(file)) {
      os.write(0);
    }
    Assert.assertTrue(service.isEngineRegistered());

    // store callback
    String key = service.storeRedirUrl("http://foo");

    // fetch callback
    Assert.assertEquals("http://foo", service.getRedirUrl(key));
  }

  @Test
  public void testHandleEngineRegistration() throws Exception {
    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterServiceImpl service = createService(file);
    service = Mockito.spy(service);

    // initiate registration

    AsterAuthorizationRequest asterRequest = new AsterAuthorizationRequest().setParameters(new AsterAuthorizationRequest.Parameters());

    AsterRestClientImpl asterRest = Mockito.mock(AsterRestClientImpl.class);
    Mockito.when(asterRest.createEngineAuthorizationRequest(Mockito.eq("http://e"), Mockito.eq("http://original"))).thenReturn(asterRequest);
    Mockito.when(service.getRestClient()).thenReturn(asterRest);

    String lState = service.storeRedirUrl("http://original");

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getParameter(Mockito.eq("lstate"))).thenReturn(lState);

    CharArrayWriter writer = new CharArrayWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(res.getWriter()).thenReturn(printWriter);

    Assert.assertNull(service.handleEngineRegistration("http://e", req, res));
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_OK));
    Mockito.verify(res, Mockito.times(1)).setContentType(Mockito.eq("application/json"));
    printWriter.close();
    asterRequest = new ObjectMapper().readValue(writer.toString(), AsterAuthorizationRequest.class);
    Assert.assertNotNull(asterRequest);
    Assert.assertNotNull(asterRequest.getParameters());

    // complete registration

    Mockito.when(req.getMethod()).thenReturn("POST");
    Mockito.when(req.getParameter(Mockito.eq("lstate"))).thenReturn(null);
    Mockito.when(req.getParameter(Mockito.eq("state"))).thenReturn("STATE");
    Mockito.when(req.getParameter(Mockito.eq("code"))).thenReturn("CODE");

    asterRequest = new AsterAuthorizationRequest().setLocalState("http://original");
    Mockito.when(asterRest.findRequest(Mockito.eq("STATE"))).thenReturn(asterRequest);

    String redirUrl = service.handleEngineRegistration("http://e", req, res);
    Assert.assertEquals("http://original", redirUrl);
    Mockito.verify(asterRest, Mockito.times(1)).registerEngine(Mockito.eq(asterRequest), Mockito.eq("CODE"));

    Mockito.verify(hook).onSuccessfulRegistration("http://original");
  }

  @Test
  public void testUserLogin() throws IOException {
    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterServiceImpl service = createService(file);
    service = Mockito.spy(service);

    // initiate user login

    AsterAuthorizationRequest asterRequest = new AsterAuthorizationRequest().setParameters(new AsterAuthorizationRequest.Parameters());

    AsterRestClientImpl asterRest = Mockito.mock(AsterRestClientImpl.class);
    Mockito.when(asterRest.createUserAuthorizationRequest(Mockito.eq("http://e"), Mockito.eq("http://original"))).thenReturn(asterRequest);
    Mockito.when(service.getRestClient()).thenReturn(asterRest);

    String lState = service.storeRedirUrl("http://original");

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getParameter(Mockito.eq("lstate"))).thenReturn(lState);

    CharArrayWriter writer = new CharArrayWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(res.getWriter()).thenReturn(printWriter);
    Assert.assertNull(service.handleUserLogin("http://e", req, res));
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_OK));
    Mockito.verify(res, Mockito.times(1)).setContentType(Mockito.eq("application/json"));
    printWriter.close();
    asterRequest = new ObjectMapper().readValue(writer.toString(), AsterAuthorizationRequest.class);
    Assert.assertNotNull(asterRequest);
    Assert.assertNotNull(asterRequest.getParameters());

    // complete user login

    Mockito.when(req.getMethod()).thenReturn("POST");
    Mockito.when(req.getParameter(Mockito.eq("lstate"))).thenReturn(null);
    Mockito.when(req.getParameter(Mockito.eq("state"))).thenReturn("STATE");
    Mockito.when(req.getParameter(Mockito.eq("code"))).thenReturn("CODE");

    asterRequest = new AsterAuthorizationRequest().setLocalState("http://original");
    Mockito.when(asterRest.findRequest(Mockito.eq("STATE"))).thenReturn(asterRequest);

    AsterUser user = Mockito.mock(AsterUser.class);
    Mockito.when(asterRest.getUserInfo(Mockito.eq(asterRequest), Mockito.eq("CODE"))).thenReturn(user);

    Assert.assertEquals(user, service.handleUserLogin("http://e", req, res));

    Mockito.verifyNoMoreInteractions(hook);
  }

  @Test
  public void testHandleLogout() throws Exception {
    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterServiceImpl service = createService(file);

    CharArrayWriter writer = new CharArrayWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    Mockito.when(res.getWriter()).thenReturn(printWriter);

    service.handleLogout(null, res);

    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_OK));
    Mockito.verify(res, Mockito.times(1)).setContentType(Mockito.eq(AsterServiceImpl.APPLICATION_JSON_MIME_TYPE));
    printWriter.close();
    AsterLogoutRequest logoutRequest = new ObjectMapper().readValue(writer.toString(), AsterLogoutRequest.class);
    Assert.assertNotNull(logoutRequest);
    Assert.assertEquals("http://dummy-aster-url:1234/logout", logoutRequest.getRedirect_uri());
  }

}
