/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.util.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

public class TestRemoteSSOService {

  @Test
  public void testDefaultConfigs() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    Mockito.doNothing().when(service).fetchInfoForClientServices();

    service.setConfiguration(new Configuration());
    Assert.assertEquals(RemoteSSOService.DPM_BASE_URL_DEFAULT + "/security/login", service.getLoginUrl());
    Assert.assertEquals(RemoteSSOService.DPM_BASE_URL_DEFAULT + "/security/_logout", service.getLogoutUrl());
    Assert.assertEquals(RemoteSSOService.DPM_BASE_URL_DEFAULT + "/security/public-rest/v1/for-client-services",
        service.getForServicesUrl()
    );
    Assert.assertEquals(RemoteSSOService.INITIAL_FETCH_INFO_FREQUENCY, service.getSecurityInfoFetchFrequency());
    Assert.assertFalse(service.hasAuthToken());
  }


  @Test
  public void testCustomConfigs() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    Mockito.doNothing().when(service).fetchInfoForClientServices();

    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_AUTH_TOKEN_CONFIG, "authToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 1);
    service.setConfiguration(conf);
    Assert.assertEquals("http://foo/security/login", service.getLoginUrl());
    Assert.assertEquals("http://foo/security/_logout", service.getLogoutUrl());
    Assert.assertEquals("http://foo/security/public-rest/v1/for-client-services", service.getForServicesUrl());
    Assert.assertEquals(RemoteSSOService.INITIAL_FETCH_INFO_FREQUENCY, service.getSecurityInfoFetchFrequency());
    Assert.assertTrue(service.hasAuthToken());
  }

  @Test
  public void testCreateRedirectionToLoginUrl() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    Mockito.doNothing().when(service).fetchInfoForClientServices();

    service.setConfiguration(new Configuration());
    Assert.assertEquals(
        RemoteSSOService.DPM_BASE_URL_DEFAULT +
            "/security/login?" +
            SSOConstants.REQUESTED_URL_PARAM +
            "=" +
            "http%3A%2F%2Ffoo%2Fsecurity",
        service.createRedirectToLoginUrl("http://foo/security", false)
    );
    Assert.assertEquals(
        RemoteSSOService.DPM_BASE_URL_DEFAULT +
            "/security/login?" +
            SSOConstants.REQUESTED_URL_PARAM +
            "=" +
            "http%3A%2F%2Ffoo%2Fsecurity&" + SSOConstants.REPEATED_REDIRECT_PARAM + "=",
        service.createRedirectToLoginUrl("http://foo/security", true)
    );
  }

  @Test
  public void testPlainTokenParser() throws Exception {
    testTokenParser(PlainSSOTokenParser.TYPE);
  }

  @Test
  public void testSignedTokenParser() throws Exception {
    testTokenParser(SignedSSOTokenParser.TYPE);
  }

  @SuppressWarnings("unchecked")
  private void testTokenParser(String type) throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());

    Map dummyData = new HashMap();
    dummyData.put(SSOConstants.TOKEN_VERIFICATION_TYPE, type);
    dummyData.put(SSOConstants.FETCH_INFO_FREQUENCY, 1);
    dummyData.put(SSOConstants.INVALIDATE_USER_AUTH_TOKENS, ImmutableList.of("a"));
    dummyData.put(SSOConstants.TOKEN_VERIFICATION_DATA, "pk");
    String dummyJson = new ObjectMapper().writeValueAsString(dummyData);
    InputStream dummyInput = new ByteArrayInputStream(dummyJson.getBytes());
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getInputStream()).thenReturn(dummyInput);
    Mockito.doReturn(conn).when(service).getSecurityInfoConnection();

    service.setConfiguration(new Configuration());
    Assert.assertEquals(type, service.getTokenParser().getType());
  }

  @Test
  public void testIsTimeToFetch() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    Mockito.doNothing().when(service).fetchInfoForClientServices();

    service.setConfiguration(new Configuration());
    Mockito.doNothing().when(service).fetchInfoForClientServices();
    Assert.assertTrue(service.isTimeToRefresh());
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo"));
    service.refresh();
    Assert.assertFalse(service.isTimeToRefresh());
  }

  @Test
  public void testRefresh() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_NO_CONTENT);
    Mockito.doReturn(conn).when(service).getSecurityInfoConnection();


    Mockito.verify(service, Mockito.never()).fetchInfoForClientServices();
    service.setConfiguration(new Configuration());
    Mockito.verify(service, Mockito.atMost(1)).fetchInfoForClientServices();
    service.refresh();
    Mockito.verify(service, Mockito.atMost(2)).fetchInfoForClientServices();
    service.refresh();
    Mockito.verify(service, Mockito.atMost(2)).fetchInfoForClientServices();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFetchInfoForClientServices() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    Mockito.doNothing().when(service).fetchInfoForClientServices();
    service.setConfiguration(new Configuration());

    Mockito.doCallRealMethod().when(service).fetchInfoForClientServices();

    Map dummyData = new HashMap();
    dummyData.put(SSOConstants.TOKEN_VERIFICATION_TYPE, PlainSSOTokenParser.TYPE);
    dummyData.put(SSOConstants.FETCH_INFO_FREQUENCY, 1);
    dummyData.put(SSOConstants.INVALIDATE_USER_AUTH_TOKENS, ImmutableList.of("a"));
    dummyData.put(SSOConstants.TOKEN_VERIFICATION_DATA, "pk");
    String dummyJson = new ObjectMapper().writeValueAsString(dummyData);
    InputStream dummyInput = new ByteArrayInputStream(dummyJson.getBytes());
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getInputStream()).thenReturn(dummyInput);
    Mockito.doReturn(conn).when(service).getSecurityInfoConnection();

    SSOTokenParser parser = Mockito.mock(SSOTokenParser.class);
    Mockito.doReturn(parser).when(service).getTokenParser();

    service.fetchInfoForClientServices();

    Mockito.verify(conn).setUseCaches(Mockito.eq(false));
    Mockito.verify(conn).setConnectTimeout(Mockito.eq(1000));
    Mockito.verify(conn).setReadTimeout(Mockito.eq(1000));

    ArgumentCaptor<String> publicKey = ArgumentCaptor.forClass(String.class);
    Mockito.verify(parser).setVerificationData(publicKey.capture());
    Assert.assertEquals("pk", publicKey.getValue());

    Assert.assertEquals(1, service.getSecurityInfoFetchFrequency());
  }

  @Test
  public void testValidateAppTokenOK() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_AUTH_TOKEN_CONFIG, "serviceToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 1);
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    Mockito.doNothing().when(service).fetchInfoForClientServices();
    service.setConfiguration(conf);

    Assert.assertEquals("http://foo/security/rest/v1/componentAuth",
        service.getAuthTokeValidationConnection().getURL().toString()
    );

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Mockito.when(conn.getOutputStream()).thenReturn(outputStream);

    SSOUserPrincipal principal = TestSSOUserPrincipalJson.createPrincipal();
    ByteArrayOutputStream responseData = new ByteArrayOutputStream();
    new ObjectMapper().writeValue(responseData, principal);
    responseData.close();
    InputStream inputStream = new ByteArrayInputStream(responseData.toByteArray());
    Mockito.when(conn.getInputStream()).thenReturn(inputStream);

    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    Mockito.doReturn(conn).when(service).getAuthTokeValidationConnection();

    SSOUserPrincipal got = service.validateAppToken("appToken", "componentId");

    ComponentAuthJson componentAuth =
        new ObjectMapper().readValue(new ByteArrayInputStream(outputStream.toByteArray()), ComponentAuthJson.class);
    Assert.assertEquals("appToken", componentAuth.getAuthToken());
    Assert.assertEquals("componentId", componentAuth.getComponentId());

    Assert.assertNotNull(got);
    Assert.assertEquals(principal, got);
    Assert.assertTrue(((SSOUserPrincipalJson)got).isLocked());

    Mockito.verify(conn).setRequestMethod(Mockito.eq("POST"));
    Mockito.verify(conn).setDoOutput(Mockito.eq(true));
    Mockito.verify(conn).setDoInput(Mockito.eq(true));
    Mockito.verify(conn).setUseCaches(Mockito.eq(false));
    Mockito.verify(conn).setConnectTimeout(Mockito.eq(1000));
    Mockito.verify(conn).setReadTimeout(Mockito.eq(1000));
    Mockito.verify(conn).setRequestProperty(Mockito.eq(SSOConstants.X_REST_CALL), Mockito.eq("-"));
    Mockito.verify(conn).setRequestProperty(Mockito.eq(SSOConstants.X_APP_AUTH_TOKEN), Mockito.eq("serviceToken"));
  }

  @Test
  public void testValidateAppTokenNoHttpOK() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_AUTH_TOKEN_CONFIG, "serviceToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 1);
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    Mockito.doNothing().when(service).fetchInfoForClientServices();
    service.setConfiguration(conf);

    Assert.assertEquals("http://foo/security/rest/v1/componentAuth",
        service.getAuthTokeValidationConnection().getURL().toString()
    );

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Mockito.when(conn.getOutputStream()).thenReturn(outputStream);

    SSOUserPrincipal principal = TestSSOUserPrincipalJson.createPrincipal();
    ByteArrayOutputStream responseData = new ByteArrayOutputStream();
    new ObjectMapper().writeValue(responseData, principal);
    responseData.close();
    InputStream inputStream = new ByteArrayInputStream(responseData.toByteArray());
    Mockito.when(conn.getInputStream()).thenReturn(inputStream);

    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_FORBIDDEN);

    Mockito.doReturn(conn).when(service).getAuthTokeValidationConnection();

    SSOUserPrincipal got = service.validateAppToken("appToken", "componentId");
    Assert.assertNull(got);
  }

  @Test
  public void testValidateAppTokenIOEx() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_AUTH_TOKEN_CONFIG, "serviceToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 1);
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    Mockito.doNothing().when(service).fetchInfoForClientServices();
    service.setConfiguration(conf);

    Assert.assertEquals("http://foo/security/rest/v1/componentAuth",
        service.getAuthTokeValidationConnection().getURL().toString()
    );

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);

    Mockito.when(conn.getOutputStream()).thenThrow(new IOException());
    SSOUserPrincipal got = service.validateAppToken("appToken", "componentId");
    Assert.assertNull(got);
  }

}
