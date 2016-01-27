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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRemoteSSOService {

  @Test
  public void testDefaultConfigs() throws Exception {
    RemoteSSOService service = new RemoteSSOService(new Configuration());
    Assert.assertEquals(RemoteSSOService.SECURITY_SERVICE_BASE_URL_DEFAULT + "/login", service.getLoginPageUrl());
    Assert.assertEquals(RemoteSSOService.SECURITY_SERVICE_BASE_URL_DEFAULT + "/public-rest/v1/for-client-services",
        service.getForServicesUrl()
    );
    Assert.assertEquals(RemoteSSOService.INITIAL_FETCH_INFO_FREQUENCY, service.getSecurityInfoFetchFrequency());
    Assert.assertNull(service.getTokenParser());
  }


  @Test
  public void testCustomConfigs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(RemoteSSOService.SECURITY_SERVICE_BASE_URL_CONFIG, "http://foo");
    RemoteSSOService service = new RemoteSSOService(conf);
    Assert.assertEquals("http://foo/login", service.getLoginPageUrl());
    Assert.assertEquals("http://foo/public-rest/v1/for-client-services", service.getForServicesUrl());
    Assert.assertEquals(RemoteSSOService.INITIAL_FETCH_INFO_FREQUENCY, service.getSecurityInfoFetchFrequency());
    Assert.assertNull(service.getTokenParser());
  }

  @Test
  public void testCreateRedirectionToLoginUrl() throws Exception {
    RemoteSSOService service = new RemoteSSOService(new Configuration());
    Assert.assertEquals(RemoteSSOService.SECURITY_SERVICE_BASE_URL_DEFAULT +
        "/login?" +
        SSOConstants.REQUESTED_URL_PARAM +
        "=" +
        "http%3A%2F%2Ffoo", service.createRedirectToLoginURL("http://foo"));
  }

  @Test
  public void testPlainTokenParser() throws Exception {
    testTokenParser(PlainSSOTokenParser.TYPE);
  }

  @Test
  public void testSignedTokenParser() throws Exception {
    testTokenParser(SignedSSOTokenParser.TYPE);
  }

  private void testTokenParser(String type) throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService(new Configuration()));

    Map dummyData = new HashMap();
    dummyData.put(SSOConstants.TOKEN_VERIFICATION_TYPE, type);
    dummyData.put(SSOConstants.FETCH_INFO_FREQUENCY, 1);
    dummyData.put(SSOConstants.INVALIDATE_TOKEN_IDS, ImmutableList.of("a"));
    dummyData.put(SSOConstants.TOKEN_VERIFICATION_DATA, "pk");
    String dummyJson = new ObjectMapper().writeValueAsString(dummyData);
    InputStream dummyInput = new ByteArrayInputStream(dummyJson.getBytes());
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getInputStream()).thenReturn(dummyInput);
    Mockito.doReturn(conn).when(service).getSecurityInfoConnection();
    service.init();
    Assert.assertEquals(type, service.getTokenParser().getType());

  }

  @Test
  public void testIsTimeToFetch() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService(new Configuration()));
    Mockito.doNothing().when(service).fetchInfoForClientServices();
    Assert.assertTrue(service.isTimeToRefresh());
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo"));
    service.refresh();
    Assert.assertFalse(service.isTimeToRefresh());
  }

  @Test
  public void testRefresh() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService(new Configuration()));

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_NO_CONTENT);
    Mockito.doReturn(conn).when(service).getSecurityInfoConnection();

    Mockito.verify(service, Mockito.never()).fetchInfoForClientServices();
    service.refresh();
    Mockito.verify(service, Mockito.atMost(1)).fetchInfoForClientServices();
    service.refresh();
    Mockito.verify(service, Mockito.atMost(1)).fetchInfoForClientServices();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFetchInfoForClientServices() throws Exception {
    RemoteSSOService service = Mockito.spy(new RemoteSSOService(new Configuration()));

    Map dummyData = new HashMap();
    dummyData.put(SSOConstants.TOKEN_VERIFICATION_TYPE, PlainSSOTokenParser.TYPE);
    dummyData.put(SSOConstants.FETCH_INFO_FREQUENCY, 1);
    dummyData.put(SSOConstants.INVALIDATE_TOKEN_IDS, ImmutableList.of("a"));
    dummyData.put(SSOConstants.TOKEN_VERIFICATION_DATA, "pk");
    String dummyJson = new ObjectMapper().writeValueAsString(dummyData);
    InputStream dummyInput = new ByteArrayInputStream(dummyJson.getBytes());
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getInputStream()).thenReturn(dummyInput);
    Mockito.doReturn(conn).when(service).getSecurityInfoConnection();

    SSOTokenParser parser = Mockito.mock(SSOTokenParser.class);
    Mockito.doReturn(parser).when(service).getTokenParser();

    SSOService.Listener listener = Mockito.mock(SSOService.Listener.class);
    service.setListener(listener);

    service.fetchInfoForClientServices();

    ArgumentCaptor<String> publicKey = ArgumentCaptor.forClass(String.class);
    Mockito.verify(parser).setVerificationData(publicKey.capture());
    Assert.assertEquals("pk", publicKey.getValue());

    ArgumentCaptor<List> invalidate = ArgumentCaptor.forClass(List.class);
    Mockito.verify(listener).invalidate(invalidate.capture());
    Assert.assertEquals(ImmutableList.of("a"), invalidate.getValue());

    Assert.assertEquals(1, service.getSecurityInfoFetchFrequency());

  }


}
