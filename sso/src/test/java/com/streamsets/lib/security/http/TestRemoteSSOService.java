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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.util.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class TestRemoteSSOService {

  @Test
  public void testDefaultConfigs() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());

    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "authToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    service.setConfiguration(conf);
    Assert.assertEquals(RemoteSSOService.DPM_BASE_URL_DEFAULT + "/security/login", service.getLoginPageUrl());
    Assert.assertEquals(RemoteSSOService.DPM_BASE_URL_DEFAULT + "/security/_logout", service.getLogoutUrl());
    Assert.assertEquals(60000, service.getConnectionTimeout());
  }


  @Test
  public void testCustomConfigs() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());

    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "authToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 30);
    conf.set(RemoteSSOService.SECURITY_SERVICE_CONNECTION_TIMEOUT_CONFIG, 2000);
    service.setConfiguration(conf);
    Assert.assertEquals("http://foo/security/login", service.getLoginPageUrl());
    Assert.assertEquals("http://foo/security/_logout", service.getLogoutUrl());
    Assert.assertEquals(2000, service.getConnectionTimeout());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLowValidateAuthTokenFrequency() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());

    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "authToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 29);
    conf.set(RemoteSSOService.SECURITY_SERVICE_CONNECTION_TIMEOUT_CONFIG, 2000);
    service.setConfiguration(conf);
  }

  @Test
  public void testValidateUserTokenWithSecurityService() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "serviceToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 30);
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    service.setConfiguration(conf);
    Mockito.doReturn(true).when(service).checkServiceActive();

    SSOPrincipalJson principal = TestSSOPrincipalJson.createPrincipal();

    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    RestClient restClient = Mockito.mock(RestClient.class);
    RestClient.Builder builder = Mockito.mock(RestClient.Builder.class);
    Mockito.doReturn(restClient).when(builder).build();
    Mockito.doReturn(builder).when(service).getUserAuthClientBuilder();
    Mockito.doReturn(response).when(restClient).post(Mockito.any());

    // valid token
    Mockito.when(response.getStatus()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(response.getData(Mockito.eq(SSOPrincipalJson.class))).thenReturn(principal);

    Assert.assertEquals(principal, service.validateUserTokenWithSecurityService("foo"));
    Assert.assertEquals("foo", principal.getTokenStr());

    // invalid token

    Mockito.when(response.getData(Mockito.eq(SSOPrincipalJson.class))).thenReturn(null);

    Assert.assertNull(service.validateUserTokenWithSecurityService("foo"));
  }

  @Test
  public void testValidateAppTokenWithSecurityService() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "serviceToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 30);
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    service.setConfiguration(conf);
    Mockito.doReturn(true).when(service).checkServiceActive();

    SSOPrincipalJson principal = TestSSOPrincipalJson.createPrincipal();

    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    RestClient restClient = Mockito.mock(RestClient.class);
    RestClient.Builder builder = Mockito.mock(RestClient.Builder.class);
    Mockito.doReturn(restClient).when(builder).build();
    Mockito.doReturn(builder).when(service).getAppAuthClientBuilder();
    Mockito.doReturn(response).when(restClient).post(Mockito.any());

    // valid token
    Mockito.when(response.getStatus()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(response.getData(Mockito.eq(SSOPrincipalJson.class))).thenReturn(principal);

    Assert.assertEquals(principal, service.validateAppTokenWithSecurityService("foo", "bar"));
    Assert.assertEquals("foo", principal.getTokenStr());

    // invalid token

    Mockito.when(response.getData(Mockito.eq(SSOPrincipalJson.class))).thenReturn(null);

    Assert.assertNull(service.validateAppTokenWithSecurityService("foo", "bar"));
  }

  @Test
  public void testRegisterOK() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    service.setConfiguration(conf);
    service.setApplicationAuthToken("appToken");
    service.setComponentId("componentId");

    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    RestClient restClient = Mockito.mock(RestClient.class);
    RestClient.Builder builder = Mockito.mock(RestClient.Builder.class);
    Mockito.doReturn(restClient).when(builder).build();
    Mockito.doReturn(builder).when(service).getRegisterClientBuilder();
    Mockito.doReturn(response).when(restClient).post(Mockito.any());

    Mockito.when(response.getStatus()).thenReturn(HttpURLConnection.HTTP_OK);

    Map<String, String> attributes = ImmutableMap.of("a", "A");
    service.register(attributes);

    ArgumentCaptor<Map> registrationData = ArgumentCaptor.forClass(Map.class);

    Mockito.verify(restClient, Mockito.times(1)).post(registrationData.capture());
    Assert.assertNotNull(registrationData.getValue());
    Assert.assertEquals("appToken", registrationData.getValue().get("authToken"));
    Assert.assertEquals("componentId", registrationData.getValue().get("componentId"));
    Assert.assertEquals(attributes, registrationData.getValue().get("attributes"));
  }

  @Test(expected = RuntimeException.class)
  public void testRegisterForbidden() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    service.setConfiguration(conf);
    service.setApplicationAuthToken("appToken");
    service.setComponentId("componentId");

    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    RestClient restClient = Mockito.mock(RestClient.class);
    RestClient.Builder builder = Mockito.mock(RestClient.Builder.class);
    Mockito.doReturn(restClient).when(builder).build();
    Mockito.doReturn(builder).when(service).getRegisterClientBuilder();
    Mockito.doReturn(response).when(restClient).post(Mockito.any());

    Mockito.when(response.getStatus()).thenReturn(HttpURLConnection.HTTP_FORBIDDEN);

    Map<String, String> attributes = ImmutableMap.of("a", "A");
    service.register(attributes);

  }

  private void testRegisterRetries(boolean unavailable) throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://notAValidDPMURL");
    conf.set(RemoteSSOService.DPM_REGISTRATION_RETRY_ATTEMPTS, "7");

    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    service.setConfiguration(conf);
    service.setApplicationAuthToken("appToken");
    service.setComponentId("componentId");
    Mockito.doNothing().when(service).sleep(Mockito.anyInt());

    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    RestClient restClient = Mockito.mock(RestClient.class);
    RestClient.Builder builder = Mockito.mock(RestClient.Builder.class);
    Mockito.doReturn(restClient).when(builder).build();
    Mockito.doReturn(builder).when(service).getRegisterClientBuilder();
    Mockito.doReturn(response).when(restClient).post(Mockito.any());

    if (unavailable) {
      Mockito.when(response.getStatus()).thenReturn(HttpURLConnection.HTTP_UNAVAILABLE);
    } else {
      Mockito.when(response.getStatus()).thenThrow(new IOException());
    }
    service.register(Collections.<String, String>emptyMap());
    Assert.assertFalse(service.isServiceActive(false));
    ArgumentCaptor<Integer> sleepCaptor = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(service, Mockito.times(6)).sleep(sleepCaptor.capture());
    Assert.assertEquals(ImmutableList.of(2, 4, 8, 16, 16, 16), sleepCaptor.getAllValues());
  }

  @Test
  public void testRegisterRetriesUnavailable() throws Exception {
    testRegisterRetries(true);
  }

  @Test
  public void testRegisterRetriesException() throws Exception {
    testRegisterRetries(false);
  }

  @Test
  public void testServiceActive() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    RemoteSSOService service = Mockito.spy(new RemoteSSOService());
    service.setConfiguration(conf);
    service.setApplicationAuthToken("appToken");
    service.setComponentId("componentId");

    Assert.assertFalse(service.isServiceActive(false));
    Mockito.verify(service, Mockito.never()).checkServiceActive();
    Mockito.verify(service, Mockito.never()).getLoginPageUrl();
    Assert.assertFalse(service.isServiceActive(true));
    Mockito.verify(service, Mockito.times(1)).checkServiceActive();
    Mockito.verify(service, Mockito.times(1)).getLoginPageUrl();
  }


  @Test
  public void testRegistrationMove() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://notAValidDPMURL");
    conf.set(RemoteSSOService.DPM_REGISTRATION_RETRY_ATTEMPTS, "7");

    RemoteSSOService service = new RemoteSSOService();
    service.setConfiguration(conf);
    service = Mockito.spy(service); // we mock it after setConfiguration so createRestClientBuilders is not tracked until after.
    service.setApplicationAuthToken("appToken");
    service.setComponentId("componentId");
    Mockito.doNothing().when(service).sleep(Mockito.anyInt());

    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    RestClient restClient = Mockito.mock(RestClient.class);
    RestClient.Builder builder = Mockito.mock(RestClient.Builder.class);
    Mockito.doReturn(restClient).when(builder).build();
    Mockito.doReturn(builder).when(service).getRegisterClientBuilder();
    Mockito.doReturn(response).when(restClient).post(Mockito.any());

    Mockito.when(response.getStatus()).thenReturn(RemoteSSOService.HTTP_PERMANENT_REDIRECT_STATUS, HttpURLConnection.HTTP_OK);
    Mockito.when(response.getHeader(Mockito.eq(RemoteSSOService.HTTP_LOCATION_HEADER))).thenReturn("http://bar/xyz");

    Mockito.doNothing().when(service).createRestClientBuilders(Mockito.eq("http://bar"));
    Mockito.doNothing().when(service).persistNewDpmBaseUrl(Mockito.eq("http://bar"));
    service.register(Collections.<String, String>emptyMap());
    ArgumentCaptor<Map> registrationData = ArgumentCaptor.forClass(Map.class);

    Mockito.verify(restClient, Mockito.times(2)).post(registrationData.capture());

    Mockito.verify(service, Mockito.times(1)).persistNewDpmBaseUrl(Mockito.eq("http://bar"));
    Mockito.verify(service, Mockito.times(1)).createRestClientBuilders(Mockito.eq("http://bar"));
  }

  @Test
  public void testValidateMove() throws Exception {

    // it is the same code for user and app validation, so testing it just once

    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration conf = Mockito.spy(new Configuration());
    Mockito.doReturn(dir).when(conf).getFilRefsBaseDir();

    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "serviceToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 30);
    RemoteSSOService service = new RemoteSSOService();
    service.setConfiguration(conf);
    service = Mockito.spy(service); // we mock it after setConfiguration so createRestClientBuilders is not tracked until after.

    Mockito.doReturn(true).when(service).checkServiceActive();

    SSOPrincipalJson principal = TestSSOPrincipalJson.createPrincipal();

    RestClient.Response response = Mockito.mock(RestClient.Response.class);
    RestClient restClient = Mockito.mock(RestClient.class);
    RestClient.Builder builder = Mockito.mock(RestClient.Builder.class);
    Mockito.doReturn(restClient).when(builder).build();
    Mockito.doReturn(builder).when(service).getAppAuthClientBuilder();
    Mockito.doReturn(response).when(restClient).post(Mockito.any());

    // MOVED on first and valid token on second call
    Mockito.when(response.getStatus()).thenReturn(RemoteSSOService.HTTP_PERMANENT_REDIRECT_STATUS, HttpURLConnection.HTTP_OK);

    Mockito.when(response.getHeader(Mockito.eq(RemoteSSOService.HTTP_LOCATION_HEADER))).thenReturn("http://bar/xyz");
    Mockito.when(response.getData(Mockito.eq(SSOPrincipalJson.class))).thenReturn(principal);

    Assert.assertEquals(principal, service.validateAppTokenWithSecurityService("foo", "bar"));
    Assert.assertEquals("foo", principal.getTokenStr());

    Mockito.verify(service, Mockito.times(1)).persistNewDpmBaseUrl(Mockito.eq("http://bar"));
    Mockito.verify(service, Mockito.times(1)).createRestClientBuilders(Mockito.eq("http://bar"));

    File file = new File(dir, "dpm-url.txt");
    Assert.assertTrue(file.exists());
    Assert.assertEquals("http://bar", Files.readAllLines(file.toPath()).get(0));
  }

  @Test
  public void testDpmClientInfo() throws Exception {
    // it is the same code for user and app validation, so testing it just once

    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Configuration conf = Mockito.spy(new Configuration());
    Mockito.doReturn(dir).when(conf).getFilRefsBaseDir();

    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://foo");
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "serviceToken");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "serviceComponentId");
    conf.set(RemoteSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 30);
    RemoteSSOService service = new RemoteSSOService();
    service.setConfiguration(conf);

    Assert.assertEquals("http://foo/", service.getDpmClientInfo().getDpmBaseUrl());
    Assert.assertEquals(
        ImmutableMap.of(
            SSOConstants.X_APP_COMPONENT_ID.toLowerCase(),
            "serviceComponentId",
            SSOConstants.X_APP_AUTH_TOKEN.toLowerCase(),
            "serviceToken"),
        service.getDpmClientInfo().getHeaders()
    );

    service.getDpmClientInfo().setDpmBaseUrl("http://bar");

    File file = new File(dir, "dpm-url.txt");
    Assert.assertTrue(file.exists());
    Assert.assertEquals("http://bar", Files.readAllLines(file.toPath()).get(0));

  }
}
