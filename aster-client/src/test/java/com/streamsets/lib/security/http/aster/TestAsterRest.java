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
import org.springframework.web.client.RestTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class TestAsterRest {

  @Test
  public void testClient() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("id")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAuthorizeUri("http://authorize")
        .setTokenUri("http://token")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterRest rest = new AsterRest(config, file);

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
    Assert.assertEquals("http://authorize", request.getAuthorizeUri());
    Assert.assertNotNull(request.getChallengeVerifier());
    Assert.assertEquals("FOO", request.getLocalState());
    Assert.assertNotNull(request.getParameters().getState());
    Assert.assertEquals("DPLANE", request.getParameters().getScope());
    Assert.assertEquals("id", request.getParameters().getClient_id());
    Assert.assertEquals(AsterRestConfig.SubjectType.DC.name(), request.getParameters().getClient_type());
    Assert.assertEquals("version", request.getParameters().getClient_version());
    Assert.assertEquals(rest.computeChallenge(request.getChallengeVerifier()), request.getParameters().getCode_challenge());
    Assert.assertEquals("S256", request.getParameters().getCode_challenge_method());
    Assert.assertEquals("http://e/registrationcallback", request.getParameters().getRedirect_uri());
    Assert.assertEquals("code", request.getParameters().getResponse_type());

    Assert.assertEquals(request, rest.findRequest(request.getParameters().getState()));

    request = rest.createUserAuthorizationRequest("http://e", "FOO");
    Assert.assertNotNull(request);
    Assert.assertEquals("http://authorize", request.getAuthorizeUri());
    Assert.assertNotNull(request.getChallengeVerifier());
    Assert.assertEquals("FOO", request.getLocalState());
    Assert.assertNotNull(request.getParameters().getState());
    Assert.assertEquals("DPLANE", request.getParameters().getScope());
    Assert.assertEquals("id", request.getParameters().getClient_id());
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
    AsterRestConfig config = new AsterRestConfig().setClientId("id")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAuthorizeUri("http://authorize")
        .setTokenUri("http://token")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterRest rest = new AsterRest(config, file);
    rest = Mockito.spy(rest);

    RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    Mockito.doReturn(restTemplate).when(rest).createRestTemplate();

    AsterAuthorizationRequest request = rest.createEngineAuthorizationRequest("http://e", "FOO");

    ResponseEntity<AsterTokenResponse> responseEntity = Mockito.mock(ResponseEntity.class);
    AsterTokenResponse response = Mockito.mock(AsterTokenResponse.class);
    Mockito.when(responseEntity.getBody()).thenReturn(response);
    Mockito.when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
    Mockito.when(restTemplate.exchange(
        Mockito.eq(config.getTokenUri()),
        Mockito.eq(HttpMethod.POST),
        Mockito.any(HttpEntity.class),
        Mockito.eq(AsterTokenResponse.class)
        )).thenReturn(responseEntity);

    Assert.assertEquals(response, rest.requestToken(request, "CODE"));
  }

  // used jwt.io to generate it
  private static final String TOKEN_WITH_ORG = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0X28iOiJvcmcifQ.RXFWZSZMrtMunxQK3JybspCemyFILM5XQzArEhJfynl9IHWRz12B9WYw2r-h9DZAHuzJhNqoIEHy71mIB3pLeKjQNd0GZ416Q-Jg1VdRq6Fx1wVPd7c0-dRuGvDQk3Kl8HS4m0U0EN7q99MG1DvXTSU-9Gzv1UFx97t5oILY5c7YWc8DSUz5H5RRlNCzwdTtO-GZVoRcb0wsw6hIDtzLc2B8jgQS_u1jZiyqWg2rO1buPfR3zGgbx6TFqh4ODtq5zB3tb15JF-GY5flHC2YDoOHAKSprR_8s3_QgOyiIPF3eUjqoH99rRkU7KnvDwWO4xzoWPLTg1gCzo9-RPF6LRw";

  @Test
  public void testGetTokenOrg() {
    AsterRestConfig config = new AsterRestConfig().setClientId("id")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAuthorizeUri("http://authorize")
        .setTokenUri("http://token")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");

    AsterRest rest = new AsterRest(config, file);

    Assert.assertEquals("org", rest.getTokenOrg(TOKEN_WITH_ORG));
  }

  @Test
  public void testGetEngineOrg() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("id")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAuthorizeUri("http://authorize")
        .setTokenUri("http://token")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG);
      new ObjectMapper().writeValue(os, response);
    }

    AsterRest rest = new AsterRest(config, file);

    Assert.assertEquals("org", rest.getEngineOrg());
  }

  @Test
  public void testRegisterEngineOK() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("id")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAuthorizeUri("http://authorize")
        .setTokenUri("http://token")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      new ObjectMapper().writeValue(os, response);
    }

    AsterRest rest = new AsterRest(config, file);
    rest = Mockito.spy(rest);

    AsterAuthorizationRequest request = new AsterAuthorizationRequest();
    Mockito.doReturn(response).when(rest).requestToken(Mockito.eq(request), Mockito.eq("CODE"));

    rest.registerEngine(request, "CODE");
  }

  // used jwt.io to generate it
  private static final String TOKEN_WITH_OTHER_ORG = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0X28iOiJvdGhlci1vcmcifQ.PSxd24OwKzoQ9iJiW6h98tPaVjepRjOC757VTjeWWid_DnUvn0O-qP472pf0YRP0xqs_X2Qclud1xQoiplMdW0bW3sH8D6d9E7paVqjf_AiEgWZBByXgM_Y_ZJozQ-rx1nZKkOzl3hgUyZdpv_dkCfHXEl97Hhl6VZD0p8B4AMPRxghBGxZOaHfTuxJJhTa2VMAPM_umWHuPAUEr9F9Mi6-C9LFP1Cwq5rC5KF57PSNk4DxGSXNFDzfm4RCWqzkmPaJCACSnbu6CYg63nrnM00HB5FPJlvmvpWNp-KdXFzp5Z86Y9JfG5iwhDB7ghuKfy0P-XBw7aUDnMFREX8Qk0Q";

  @Test(expected = AsterAuthException.class)
  public void testRegisterEngineFail() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("id")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAuthorizeUri("http://authorize")
        .setTokenUri("http://token")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      new ObjectMapper().writeValue(os, response);
    }

    AsterRest rest = new AsterRest(config, file);
    rest = Mockito.spy(rest);

    response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_OTHER_ORG);

    AsterAuthorizationRequest request = new AsterAuthorizationRequest();
    Mockito.doReturn(response).when(rest).requestToken(Mockito.eq(request), Mockito.eq("CODE"));

    rest.registerEngine(request, "CODE");
  }

  // used jwt.io to generate them
  private static final String USER_TOKEN_WITH_ORG = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0X3R5cGUiOiJVU0VSIiwidF9lbWFpbCI6ImZvb0Bmb28iLCJ0X28iOiJvcmciLCJ0X2Vfcm9sZXMiOlsiZW5naW5lOmFkbWluIl0sInRfZV9ncm91cHMiOlsiYWxsIl19.bO7yUWgZ21CtNns9GtpVb30Hhdad9C56cI2Vq1pGH3TcheLcVciSBt_6QnSCtnDdDTFkRdxk8wO3VZBYPKARUvu4qMLpwnJ8-ejNptA81NcD7RWo2z0HQpRLdGegfch2S8EawLYPotY_4hGYx5xv1oNouXALeYVVicpPEyVcalNI_ay6ACRtsKVlYks73AvZQ34A6IoVxj43_KvrrPxXgG3tgjLAX8OkYKKwV0N_41eUELAyoimW0gYOxB5bhJJgTAWNQb_KNTBdDgNv6LRunqNowUEC3be3YLvn3kW9T2uWhv4_BAPrxhgT4JPfKrBN1Buky2mvboyWG6rKCBB32Q";
  private static final String USER_TOKEN_WITH_OTHER_ORG = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0X3R5cGUiOiJVU0VSIiwidF9lbWFpbCI6ImZvb0Bmb28iLCJ0X28iOiJvdGhlci1vcmciLCJ0X2Vfcm9sZXMiOlsiZW5naW5lOmFkbWluIl0sInRfZV9ncm91cHMiOlsiYWxsIl19.MUUbGAtC6CjmGxo1GcnSIXXY4rTMe7ffP4zlWJ-R71S7FDfh8j_ToxGk1geZJC14Ymd6hBWqcZtddAv3X2rdPAV1QI3fWK9Ma9biCGUf8t49FbgxiWPdYhaacCt8NO33wjfPZ4xQl33lcvC5iONorgLGbMQyRaMd57WzCpvR5-r2jO-ShKVcrJvaTiFY__C_NIdGI3jhF4G5403AN5QU8vA8r5OQe1aP8_VWDsvqx7NUfWiH55lyXczbC3iCsLX2SlzFDnRxtruB2PWzgzGPhR9ESHIl-otvepYHt3wyPo95C9LXbR09ORvit5AQA3vJKx9xtwaryojVczEtjQye1A";

  @Test
  public void testGetUserInfoOk() throws IOException {
    AsterRestConfig config = new AsterRestConfig().setClientId("id")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAuthorizeUri("http://authorize")
        .setTokenUri("http://token")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_ORG);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      new ObjectMapper().writeValue(os, response);
    }

    AsterRest rest = new AsterRest(config, file);
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
    AsterRestConfig config = new AsterRestConfig().setClientId("id")
        .setClientVersion("version")
        .setSubjectType(AsterRestConfig.SubjectType.DC)
        .setLoginCallbackPath("/logincallback")
        .setRegistrationCallbackPath("/registrationcallback")
        .setAuthorizeUri("http://authorize")
        .setTokenUri("http://token")
        .setStateCacheExpirationSecs(60)
        .setAccessTokenMaxExpInSecs(600);

    AsterTokenResponse response = new AsterTokenResponse().setAccess_token(TOKEN_WITH_OTHER_ORG);

    File file = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(file.mkdir());
    file = new File(file, "store.json");
    try (OutputStream os = new FileOutputStream(file)) {
      new ObjectMapper().writeValue(os, response);
    }

    AsterRest rest = new AsterRest(config, file);
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
        if (!req.getParameter("client_id").equals("engineId")) {
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
        if (!req.getParameter("client_id").equals("engineId")) {
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
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
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
    context.addServlet(new ServletHolder(tokenServlet), "/token");
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();
      String asterUrl = "http://localhost:" + server.getURI().getPort();

      AsterRestConfig config = new AsterRestConfig().setClientId("engineId")
          .setClientVersion("version")
          .setSubjectType(AsterRestConfig.SubjectType.DC)
          .setLoginCallbackPath("/logincallback")
          .setRegistrationCallbackPath("/registrationcallback")
          .setAuthorizeUri(asterUrl + "/authorize")
          .setTokenUri(asterUrl + "/token")
          .setStateCacheExpirationSecs(60)
          .setAccessTokenMaxExpInSecs(600);

      File file = new File("target", UUID.randomUUID().toString());
      Assert.assertTrue(file.mkdir());
      file = new File(file, "store.json");

      AsterRest rest = new AsterRest(config, file);

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
    context.addServlet(new ServletHolder(tokenServlet), "/token");
    ApiServlet apiServlet = new ApiServlet();
    context.addServlet(new ServletHolder(apiServlet), "/api");
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();
      String asterUrl = "http://localhost:" + server.getURI().getPort();

      AsterRestConfig config = new AsterRestConfig().setClientId("engineId")
          .setClientVersion("version")
          .setSubjectType(AsterRestConfig.SubjectType.DC)
          .setLoginCallbackPath("/logincallback")
          .setRegistrationCallbackPath("/registrationcallback")
          .setAuthorizeUri(asterUrl + "/authorize")
          .setTokenUri(asterUrl + "/token")
          .setStateCacheExpirationSecs(60)
          .setAccessTokenMaxExpInSecs(600);

      File file = new File("target", UUID.randomUUID().toString());
      Assert.assertTrue(file.mkdir());
      file = new File(file, "store.json");

      AsterRest rest = new AsterRest(config, file);

      // with valid access token
      try (OutputStream os = new FileOutputStream(file)) {
        new ObjectMapper().writeValue(os,
            new AsterTokenResponse().setAccess_token(USER_TOKEN_WITH_ORG)
                .setExpires_in(100)
                .setExpires_on(Instant.now().plusSeconds(100).getEpochSecond())
        );
      }

      rest.doRestCall(rt -> {
        ResponseEntity<Void> response = rt.exchange(
            asterUrl + "/api",
            HttpMethod.GET,
            new HttpEntity<>(null),
            Void.class
        );
        Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
        return null;
      });

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

      rest.doRestCall(rt -> {
        ResponseEntity<Void> response = rt.exchange(
            asterUrl + "/api",
            HttpMethod.GET,
            new HttpEntity<>(null),
            Void.class
        );
        Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
        return null;
      });

      Assert.assertTrue(tokenServlet.refreshDone);

    } finally {
      server.stop();
    }

  }

}
