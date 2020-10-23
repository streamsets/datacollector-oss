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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class TestAsterRestClientImpl {

  @Test
  public void testClient() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("123")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAsterUrl("http://aster")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterRestClientImpl rest = new AsterRestClientImpl(config, file);

    // hasTokens()
    Assert.assertFalse(rest.hasTokens());

    try (OutputStream os = new FileOutputStream(file)) {
      os.write(0);
    }
    Assert.assertTrue(rest.hasTokens());

    String challenge = rest.computeChallenge("foobar");

    Assert.assertEquals(
        "YzNhYjhmZjEzNzIwZThhZDkwNDdkZDM5NDY2YjNjODk3NGU1OTJjMmZhMzgzZDRhMzk2MDcxNGNhZWYwYzRmMg==",
        challenge
    );

    AsterAuthorizationRequest request = rest.createEngineAuthorizationRequest("http://e", "FOO");
    Assert.assertNotNull(request);
    Assert.assertEquals("http://aster" + AsterRestClientImpl.ASTER_OAUTH_AUTHORIZE_PATH, request.getAuthorizeUri());
    Assert.assertNotNull(request.getChallengeVerifier());
    Assert.assertEquals("FOO", request.getLocalState());
    Assert.assertNotNull(request.getParameters().getState());
    Assert.assertEquals("DPLANE", request.getParameters().getScope());
    Assert.assertEquals("123", request.getParameters().getClient_id());
    Assert.assertEquals(AsterRestConfig.SubjectType.DC.name(), request.getParameters().getClient_type());
    Assert.assertEquals("version", request.getParameters().getClient_version());
    Assert.assertEquals(rest.computeChallenge(request.getChallengeVerifier()), request.getParameters().getCode_challenge());
    Assert.assertEquals("S256", request.getParameters().getCode_challenge_method());
    Assert.assertEquals("http://e/registrationcallback", request.getParameters().getRedirect_uri());
    Assert.assertEquals("code", request.getParameters().getResponse_type());

    Assert.assertEquals(request, rest.findRequest(request.getParameters().getState()));

    request = rest.createUserAuthorizationRequest("http://e", "FOO");
    Assert.assertNotNull(request);
    Assert.assertEquals("http://aster" + AsterRestClientImpl.ASTER_OAUTH_AUTHORIZE_PATH, request.getAuthorizeUri());
    Assert.assertNotNull(request.getChallengeVerifier());
    Assert.assertEquals("FOO", request.getLocalState());
    Assert.assertNotNull(request.getParameters().getState());
    Assert.assertEquals("DPLANE", request.getParameters().getScope());
    Assert.assertEquals("123", request.getParameters().getClient_id());
    Assert.assertEquals(AsterRestConfig.SubjectType.USER.name(), request.getParameters().getClient_type());
    Assert.assertEquals("version", request.getParameters().getClient_version());
    Assert.assertEquals(rest.computeChallenge(request.getChallengeVerifier()), request.getParameters().getCode_challenge());
    Assert.assertEquals("S256", request.getParameters().getCode_challenge_method());
    Assert.assertEquals("http://e/logincallback", request.getParameters().getRedirect_uri());
    Assert.assertEquals("code", request.getParameters().getResponse_type());

    Assert.assertEquals(request, rest.findRequest(request.getParameters().getState()));
  }

  @Test
  public void testRestTemplateRequestToken() {
    AsterRestConfig config = new AsterRestConfig().setClientId("123")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAsterUrl("http://aster")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterRestClientImpl rest = new AsterRestClientImpl(config, file);
    rest = Mockito.spy(rest);

    RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    Mockito.doReturn(restTemplate).when(rest).createRestTemplate();

    AsterAuthorizationRequest request = rest.createEngineAuthorizationRequest("http://e", "FOO");

    ResponseEntity<AsterTokenResponse> responseEntity = Mockito.mock(ResponseEntity.class);
    AsterTokenResponse response = Mockito.mock(AsterTokenResponse.class);
    Mockito.when(responseEntity.getBody()).thenReturn(response);
    Mockito.when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
    Mockito.when(restTemplate.exchange(
        Mockito.eq(config.getAsterUrl() + AsterRestClientImpl.ASTER_OAUTH_TOKEN_PATH),
        Mockito.eq(HttpMethod.POST),
        Mockito.any(HttpEntity.class),
        Mockito.eq(AsterTokenResponse.class)
        )).thenReturn(responseEntity);

    Assert.assertEquals(response, rest.requestToken(request, "CODE"));
  }

  // used jwt.io to generate it
  private static final String TOKEN_WITH_ORG =
      "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjMiLCJ0X28iOiJvcmcifQ.j9EvVI7sOkms3Pnx4zAx_aZALwDdYSIyxH4l4FnDKaV8JzJBQv3xt2vTAUuI85GDvjBTRG3MfoGFXBteebPJu9rDQ60C2jSmiIHFByEmEFQTJThdr1H8GLeHc8DJ7z5xS3-cLYKXs-h_cNAXYqhmGyr5iQk8zgmTp_PEoVFEZFuHzSAP4jn3LTrUbh60aMP7cooeKQckSLLbJUNtneolZRlTNcdiFQVFGd153LIbcw67xmwAiDPmyj_Pi-fwn7KDDpd5VZUjrAiwkGQdGCMpQMSJNkcbj1dEY-iHafi1Cb0dZIJXZ5pMo6hUWvzHeGWzo5ObxyYkGt-dFlnY9sDvng";

  @Test
  public void testGetTokenOrg() {
    AsterRestConfig config = new AsterRestConfig().setClientId("123")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAsterUrl("http://aster")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterRestClientImpl rest = new AsterRestClientImpl(config, file);

    Assert.assertEquals("org", rest.getTokenOrg(TOKEN_WITH_ORG));
  }

  @Test
  public void testGetEngineOrg() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("123")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAsterUrl("http://aster")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG);
      new ObjectMapper().writeValue(os, response);
    }

    AsterRestClientImpl rest = new AsterRestClientImpl(config, file);

    Assert.assertEquals("org", rest.getEngineOrg());
  }

  @Test
  public void testRegisterEngineOK() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("123")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAsterUrl("http://aster")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      new ObjectMapper().writeValue(os, response);
    }

    AsterRestClientImpl rest = new AsterRestClientImpl(config, file);
    rest = Mockito.spy(rest);

    AsterAuthorizationRequest request = new AsterAuthorizationRequest();
    Mockito.doReturn(response).when(rest).requestToken(Mockito.eq(request), Mockito.eq("CODE"));

    rest.registerEngine(request, "CODE");
  }

  // used jwt.io to generate it
  private static final String TOKEN_WITH_OTHER_ORG =
      "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjMiLCJ0X28iOiJvdGhlcl9vcmcifQ.LzhY6ELyQHwU2UvR5zIdCTZlYnDu1tGL4tBmwW-Z0z4ck9Lg4H2n4uQ9_pJpThYAvi7VmPRtokT8jo9Yo2AQqBopmIHy4b_QNFBQ8jQ1Uy4tkSGKxz2zdDQFXdtO24olvT94-Ssa41uauuvZZ_-mWxsN3SPUqrLdewQSvPB6HeuoTxnMZOh9dHHi-_mMc2MwbRKjJYR_-vM5vOFdMFfzIc-f_09NzZYgzhljFdymeJ2nWO1jbSI--zgIm9xlC4bmuPuA7igY1toSVqwONIwc1LOt8b7c3ga6hjQZW0ja8JaKROHmqEy1H9F6EZG3jTZ_EZKCE5VFRgpIuo-__bTqAQ";

  @Test(expected = AsterAuthException.class)
  public void testRegisterEngineFail() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("123")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAsterUrl("http://aster")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      new ObjectMapper().writeValue(os, response);
    }

    AsterRestClientImpl rest = new AsterRestClientImpl(config, file);
    rest = Mockito.spy(rest);

    response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_OTHER_ORG);

    AsterAuthorizationRequest request = new AsterAuthorizationRequest();
    Mockito.doReturn(response).when(rest).requestToken(Mockito.eq(request), Mockito.eq("CODE"));

    rest.registerEngine(request, "CODE");
  }

  // used jwt.io to generate them
  private static final String USER_TOKEN_WITH_ORG = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjMiLCJ0X3R5cGUiOiJVU0VSIiwidF9lbWFpbCI6ImZvb0Bmb28iLCJ0X28iOiJvcmciLCJ0X2Vfcm9sZXMiOlsiZW5naW5lOmFkbWluIl0sInRfZV9ncm91cHMiOlsiYWxsIl19.ZAVQlbqGHcnEcIZT0St2kAOtuz2Ajm7aYLbry1eh2Qox_k7IJQCn5BDKlUvw0CreOwlJdjBTtAaSunvEM-GqAP7fO8ZueIHjovQPZ8IGAKWLkFN4OJwNBy1qZzrtuaclfS9dL_GE7cfav2f2VwUqy2zDUvSdE95JJaEGvqINcjG4j9UiwJnNucmhyac5hzW_sSQD4Ds9jIItN1RULvaC0ZhdJT-2KumDUurKmoAcppkkSSo2GHgA-WnAsxG04QWQQHXqEja9co-wW6aPw4MHXUKquQcw5zqxZ5TT-tQ1A_98YNKSC_wRqNVzQ70H0eMGBheOW0OP9i9fSG9uKAtenA";
  private static final String USER_TOKEN_WITH_OTHER_ORG = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjMiLCJ0X3R5cGUiOiJVU0VSIiwidF9lbWFpbCI6ImZvb0Bmb28iLCJ0X28iOiJvdGhlcl9vcmciLCJ0X2Vfcm9sZXMiOlsiZW5naW5lOmFkbWluIl0sInRfZV9ncm91cHMiOlsiYWxsIl19.L3CpGwdTy2W2GVIA3rz754wa0wuyEY38ExmxIsBi9pF7swqAq34iMg0syZptKQ1BRa1vnXACpeA5od9GwUenUheoJcHpkSoioqkMuSZzuho1NXZjF3rC50D9aapcGltxkiDgFTuXmcvuhFDsdAd03V6jx-LYjiEndinI-XlxMiahBE2ptYE8ZUri5LGzx9JSk2OzsYFMGfLJa3W3sPjPuEPrStnkvCboCDLtrHx0F9QWoSi_bvErd7LRY9JRMS7hctRwWhs_Ikio5_aX80vteLtXMqXv7XewgysgfFHW2ht_EK6s_y-H7iBtXPUUtzFlYujNc0TSTwr9hMSDEtYeBg";

  @Test
  public void testGetUserInfoOk() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("123")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAsterUrl("http://aster")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      new ObjectMapper().writeValue(os, response);
    }

    AsterRestClientImpl rest = new AsterRestClientImpl(config, file);
    rest = Mockito.spy(rest);

    response = new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_ORG);

    AsterAuthorizationRequest request = new AsterAuthorizationRequest();
    Mockito.doReturn(response).when(rest).requestToken(Mockito.eq(request), Mockito.eq("CODE"));

    AsterUser user = rest.getUserInfo(request, "CODE");
    Assert.assertNotNull(user);
    Assert.assertEquals("foo@foo", user.getName());
    Assert.assertEquals("org", user.getOrg());
    Assert.assertEquals(ImmutableSet.of("user", "admin"), user.getRoles());
    Assert.assertEquals(ImmutableSet.of("all"), user.getGroups());
  }

  @Test(expected = AsterAuthException.class)
  public void testGetUserInfoFail() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("123")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAsterUrl("http://aster")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_OTHER_ORG);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      new ObjectMapper().writeValue(os, response);
    }

    AsterRestClientImpl rest = new AsterRestClientImpl(config, file);
    rest = Mockito.spy(rest);

    response = new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_ORG);

    AsterAuthorizationRequest request = new AsterAuthorizationRequest();
    Mockito.doReturn(response).when(rest).requestToken(Mockito.eq(request), Mockito.eq("CODE"));

    AsterUser user = rest.getUserInfo(request, "CODE");
    Assert.assertNotNull(user);
  }

  private static class TokenServlet extends HttpServlet {
    volatile boolean refreshDone;

    private static final Set<String> AUTHORIZATION_CODE_REQUIRED_PARAMS = ImmutableSet.of(
        "client_id",
        "client_type",
        "grant_type",
        "code_verifier",
        "code"
    );
    private static final Set<String> REFRESH_REQUIRED_PARAMS = ImmutableSet.of(
        "client_id",
        "grant_type",
        "refresh_token"
    );

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      if ("authorization_code".equals(req.getParameter("grant_type"))) {
        if (!req.getParameterMap().keySet().containsAll(AUTHORIZATION_CODE_REQUIRED_PARAMS)) {
          resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        if (!req.getParameter("client_id").equals("123")) {
          resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        AsterTokenResponse tokenResponse;
        switch (req.getParameter("code")) {
          case "new-registration":
          case "re-registration-ok":
            if (!req.getParameter("client_type").equals("DC") && !req.getParameter("client_type").equals("TF")) {
              resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
              return;
            }
            tokenResponse = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG)
                .setRefresh_token("RT")
                .setExpires_in(100)
                .setExpires_on(Instant.now().plusSeconds(100).getEpochSecond());
            break;
          case "re-registration-other-org":
            if (!req.getParameter("client_type").equals("DC") && !req.getParameter("client_type").equals("TF")) {
              resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
              return;
            }
            tokenResponse = new AsterTokenResponse().setAccess_token(TOKEN_WITH_OTHER_ORG)
                .setRefresh_token("RT")
                .setExpires_in(100)
                .setExpires_on(Instant.now().plusSeconds(100).getEpochSecond());
            break;
          case "user-ok":
            tokenResponse = new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_ORG)
                .setExpires_in(100)
                .setExpires_on(Instant.now().plusSeconds(100).getEpochSecond());
            break;
          case "user-other-org":
            tokenResponse = new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_OTHER_ORG)
                .setExpires_in(100)
                .setExpires_on(Instant.now().plusSeconds(100).getEpochSecond());
            break;
          default:
            tokenResponse = null;
        }
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        new ObjectMapper().writeValue(resp.getWriter(), tokenResponse);
      } else if ("refresh_token".equals(req.getParameter("grant_type"))) {
        if (!req.getParameterMap().keySet().containsAll(REFRESH_REQUIRED_PARAMS)) {
          resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        if (!req.getParameter("client_id").equals("123")) {
          resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        if (!req.getParameter("refresh_token").equals("RT")) {
          resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        AsterTokenResponse tokenResponse = new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_ORG)
            .setExpires_in(100)
            .setExpires_on(Instant.now().plusSeconds(100).getEpochSecond())
            .setRefresh_token("RT1");
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        new ObjectMapper().writeValue(resp.getWriter(), tokenResponse);
        refreshDone = true;
      } else {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }
    }
  }

  private static class ApiServlet extends HttpServlet {
    volatile long sleepTime = 0;
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      if (sleepTime > 0) {
        try {
          Thread.sleep(Long.valueOf(sleepTime));
        } catch (InterruptedException e) {
        }
        sleepTime = 0;
      }

      if (("Bearer " + USER_TOKEN_WITH_ORG).equals(req.getHeader("Authorization"))) {
        resp.setStatus(HttpServletResponse.SC_OK);
      } else {
        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      }
    }
  }

  @Test
  public void testEngineRegistrationAndUserInfo() throws Exception {
    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    TokenServlet tokenServlet = new TokenServlet();
    context.addServlet(new ServletHolder(tokenServlet), AsterRestClientImpl.ASTER_OAUTH_TOKEN_PATH);
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();
      String asterUrl = "http://localhost:" + server.getURI().getPort();

      AsterRestConfig config = new AsterRestConfig().setClientId("123")
          .setClientVersion("version")
          .setSubjectType(AsterRestConfig.SubjectType.DC)
          .setLoginCallbackPath("/logincallback")
          .setRegistrationCallbackPath("/registrationcallback")
          .setAsterUrl(asterUrl)
          .setStateCacheExpirationSecs(60)
          .setAccessTokenMaxExpInSecs(600);

      File file = new File("target", UUID.randomUUID().toString());
      Assert.assertTrue(file.mkdir());
      file = new File(file, "store.json");

      AsterRestClientImpl rest = new AsterRestClientImpl(config, file);

      // clean engine registration
      AsterAuthorizationRequest request = rest.createEngineAuthorizationRequest("http://e", "localState");
      rest.registerEngine(request, "new-registration");
      Assert.assertEquals("org", rest.getEngineOrg());

      // engine re registration ok
      request = rest.createEngineAuthorizationRequest("http://e", "localState");
      rest.registerEngine(request, "re-registration-ok");
      Assert.assertEquals("org", rest.getEngineOrg());

      // engine re registration other org
      request = rest.createEngineAuthorizationRequest("http://e", "localState");
      try {
        rest.registerEngine(request, "re-registration-other-org");
        Assert.fail();
      } catch (Exception ex) {
        Assert.assertEquals(AsterAuthException.class, ex.getClass());
      }
      Assert.assertEquals("org", rest.getEngineOrg());

      // user info ok
      request = rest.createUserAuthorizationRequest("http://e", "localState");
      AsterUser user = rest.getUserInfo(request, "user-ok");
      Assert.assertNotNull(user);
      Assert.assertEquals("foo@foo", user.getName());
      Assert.assertEquals("org", user.getOrg());
      Assert.assertEquals(ImmutableSet.of("user", "admin"), user.getRoles());
      Assert.assertEquals(ImmutableSet.of("all"), user.getGroups());

      // user info other org
      request = rest.createUserAuthorizationRequest("http://e", "localState");
      try {
        rest.getUserInfo(request, "user-other-org");
        Assert.fail();
      } catch (Exception ex) {
        Assert.assertEquals(AsterAuthException.class, ex.getClass());
      }

    } finally {
      server.stop();
    }

  }

  @Test
  public void testRestCall() throws Exception {
    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    TokenServlet tokenServlet = new TokenServlet();
    context.addServlet(new ServletHolder(tokenServlet), AsterRestClientImpl.ASTER_OAUTH_TOKEN_PATH);
    ApiServlet apiServlet = new ApiServlet();
    context.addServlet(new ServletHolder(apiServlet), AsterRestClientImpl.ASTER_OAUTH_AUTHORIZE_PATH);
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();
      String asterUrl = "http://localhost:" + server.getURI().getPort();

      AsterRestConfig config = new AsterRestConfig().setClientId("123")
          .setClientVersion("version")
          .setSubjectType(AsterRestConfig.SubjectType.DC)
          .setLoginCallbackPath("/logincallback")
          .setRegistrationCallbackPath("/registrationcallback")
          .setAsterUrl(asterUrl)
          .setStateCacheExpirationSecs(60)
          .setAccessTokenMaxExpInSecs(600);

      File file = new File("target", UUID.randomUUID().toString());
      Assert.assertTrue(file.mkdir());
      file = new File(file, "store.json");

      AsterRestClientImpl rest = new AsterRestClientImpl(config, file);

      // with valid access token
      try (OutputStream os = new FileOutputStream(file)) {
        new ObjectMapper().writeValue(os,
            new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_ORG)
                .setExpires_in(100)
                .setExpires_on(Instant.now().plusSeconds(100).getEpochSecond())
        );
      }

      AsterRestClient.Response<Void> response = rest.doRestCall(new AsterRestClient.Request<>()
          .setResourcePath(asterUrl + AsterRestClientImpl.ASTER_OAUTH_AUTHORIZE_PATH)
          .setRequestType(AsterRestClient.RequestType.GET)
      );

      Assert.assertEquals(response.getStatusCode(), HttpStatus.OK.value());

      Assert.assertFalse(tokenServlet.refreshDone);

      // with expired access token but valid refresh token
      try (OutputStream os = new FileOutputStream(file)) {
        new ObjectMapper().writeValue(os,
            new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_ORG)
                .setExpires_in(100)
                .setExpires_on(Instant.now().minusSeconds(100).getEpochSecond())
                .setRefresh_token("RT")
        );
      }

      response = rest.doRestCall(new AsterRestClient.Request<>()
          .setResourcePath(asterUrl + AsterRestClientImpl.ASTER_OAUTH_AUTHORIZE_PATH)
          .setRequestType(AsterRestClient.RequestType.GET)
      );
      Assert.assertEquals(response.getStatusCode(), HttpStatus.OK.value());
      Assert.assertTrue(tokenServlet.refreshDone);

    } finally {
      server.stop();
    }

  }

  @Test
  public void testTimeouts() throws Exception {
    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    ApiServlet apiServlet = new ApiServlet();
    context.addServlet(new ServletHolder(apiServlet), "/test");
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();
      String asterUrl = "http://localhost:" + server.getURI().getPort();

      AsterRestConfig config = new AsterRestConfig().setClientId("123")
          .setClientVersion("version")
          .setSubjectType(AsterRestConfig.SubjectType.DC)
          .setLoginCallbackPath("/logincallback")
          .setRegistrationCallbackPath("/registrationcallback")
          .setAsterUrl(asterUrl)
          .setStateCacheExpirationSecs(60)
          .setAccessTokenMaxExpInSecs(600);

      File file = new File("target", UUID.randomUUID().toString());
      Assert.assertTrue(file.mkdir());
      file = new File(file, "store.json");

      // with valid access token
      try (OutputStream os = new FileOutputStream(file)) {
        new ObjectMapper().writeValue(os,
            new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_ORG)
                .setExpires_in(100)
                .setExpires_on(Instant.now().plusSeconds(100).getEpochSecond())
        );
      }

      AsterRestClientImpl rest = new AsterRestClientImpl(config, file);

      long start = System.currentTimeMillis();
      AsterRestClient.Response<Void> response = rest.doRestCall(new AsterRestClient.Request<>()
          .setResourcePath(asterUrl + "/test")
          .setRequestType(AsterRestClient.RequestType.GET)
      );
      long end = System.currentTimeMillis();
      Assert.assertEquals(response.getStatusCode(), HttpStatus.OK.value());

      long duration = end - start;

      // make sure sleep is working, set timeout that is plenty high enough
      int sleepTime = (int) Math.max(duration * 2, duration + 500);
      apiServlet.sleepTime = sleepTime;
      start = System.currentTimeMillis();
      response = rest.doRestCall(new AsterRestClient.Request<>()
          .setResourcePath(asterUrl + "/test")
          .setRequestType(AsterRestClient.RequestType.GET)
          .setTimeout(sleepTime * 2)
      );
      end = System.currentTimeMillis();

      Assert.assertEquals(response.getStatusCode(), HttpStatus.OK.value());
      Assert.assertTrue("Expected duration " + (end - start) + " >= sleepTime " + sleepTime,
          end - start >= sleepTime);
      Assert.assertTrue(end - start <= (long) sleepTime * 2);

      // timeout shorter than sleep
      apiServlet.sleepTime = sleepTime;
      start = System.currentTimeMillis();
      try {
        response = rest.doRestCall(new AsterRestClient.Request<>().setResourcePath(asterUrl + "/test")
            .setRequestType(AsterRestClient.RequestType.GET)
            .setTimeout(sleepTime / 2));
        Assert.fail("expected exception");
      } catch (ResourceAccessException e) {
        end = System.currentTimeMillis();
        Assert.assertEquals(SocketTimeoutException.class, e.getCause().getClass());
      }

      Assert.assertTrue(end - start >= sleepTime / 2);
      Assert.assertTrue(end - start < sleepTime);

    } finally {
      server.stop();
    }

  }

}
