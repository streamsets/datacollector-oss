/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import java.net.URLEncoder;

public class TestAbstractSSOService {

  class ForTestSSOService extends AbstractSSOService {
    @Override
    protected SSOPrincipal validateUserTokenWithSecurityService(String authToken) {
      return null;
    }

    @Override
    protected SSOPrincipal validateAppTokenWithSecurityService(String authToken, String componentId) {
      return null;
    }
  }

  @Test
  public void testConfiguration() {
    ForTestSSOService service = Mockito.spy(new ForTestSSOService());

    Configuration conf = new Configuration();
    service.setConfiguration(conf);
    Mockito
        .verify(service)
        .initializePrincipalCaches(Mockito.eq(AbstractSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_DEFAULT *
            1000));

    conf.set(AbstractSSOService.SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, 1);
    service.setConfiguration(conf);
    Mockito.verify(service).initializePrincipalCaches(Mockito.eq(1000l));

    Assert.assertNotNull(service.getUserPrincipalCache());
    Assert.assertNotNull(service.getAppPrincipalCache());

    service.setLoginPageUrl("http://foo");
    Assert.assertEquals("http://foo", service.getLoginPageUrl());
    service.setLogoutUrl("http://bar");
    Assert.assertEquals("http://bar", service.getLogoutUrl());
  }

  @Test
  public void testCreateRedirectToLoginUrl() throws Exception {
    ForTestSSOService service = new ForTestSSOService();
    service.setConfiguration(new Configuration());
    service.setLoginPageUrl("http://foo");

    String initialRedirUrl =
        "http://foo" + "?" + SSOConstants.REQUESTED_URL_PARAM + "=" + URLEncoder.encode("http://bar", "UTF-8");
    String repeatedRedirUrl = "http://foo" +
        "?" +
        SSOConstants.REQUESTED_URL_PARAM +
        "=" +
        URLEncoder.encode("http://bar", "UTF-8") +
        "&" +
        SSOConstants.REPEATED_REDIRECT_PARAM +
        "=";
    Assert.assertEquals(initialRedirUrl, service.createRedirectToLoginUrl("http://bar", false));
    Assert.assertEquals(repeatedRedirUrl, service.createRedirectToLoginUrl("http://bar", true));
  }

  @Test
  public void testValidateUserToken() throws Exception {
    ForTestSSOService service = Mockito.spy(new ForTestSSOService());
    service.setConfiguration(new Configuration()); //60 sec cache

    //invalid, unknown
    Assert.assertNull(service.validateUserToken("x"));
    Mockito.verify(service).validateUserTokenWithSecurityService(Mockito.eq("x"));

    //invalid, cached
    Mockito.reset(service);
    Assert.assertNull(service.validateUserToken("x"));
    Mockito.verify(service, Mockito.never()).validateUserTokenWithSecurityService(Mockito.eq("x"));

    //valid, unknown
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);
    Mockito.when(service.validateUserTokenWithSecurityService(Mockito.eq("a"))).thenReturn(principal);

    Assert.assertEquals(principal, service.validateUserToken("a"));
    Mockito.verify(service).validateUserTokenWithSecurityService(Mockito.eq("a"));

    //valid, cached
    Mockito.reset(service);
    Assert.assertEquals(principal, service.validateUserToken("a"));
    Mockito.verify(service, Mockito.never()).validateUserTokenWithSecurityService(Mockito.eq("a"));

    service.initializePrincipalCaches(1); //1 millisec cache

    //valid, unknown
    Mockito.when(service.validateUserTokenWithSecurityService(Mockito.eq("b"))).thenReturn(principal);

    Assert.assertEquals(principal, service.validateUserToken("b"));
    Mockito.verify(service).validateUserTokenWithSecurityService(Mockito.eq("b"));

    // cache expired
    Thread.sleep(2);

    //valid, unknown
    Mockito.reset(service);
    Mockito.when(service.validateUserTokenWithSecurityService(Mockito.eq("b"))).thenReturn(principal);
    Assert.assertEquals(principal, service.validateUserToken("b"));
    Mockito.verify(service).validateUserTokenWithSecurityService(Mockito.eq("b"));
  }

  @Test
  public void testInvalidateUserToken() throws Exception {
    ForTestSSOService service = Mockito.spy(new ForTestSSOService());
    service.setConfiguration(new Configuration()); // 60 sec cache

    // unknown
    service.invalidateUserToken("x");
    Assert.assertNull(service.validateUserToken("x"));

    // valid, unknown
    Mockito
        .when(service.validateUserTokenWithSecurityService(Mockito.eq("y")))
        .thenReturn(Mockito.mock(SSOPrincipal.class));
    service.invalidateUserToken("y");
    Assert.assertNull(service.validateUserToken("y"));

    // valid, cached
    Mockito
        .when(service.validateUserTokenWithSecurityService(Mockito.eq("z")))
        .thenReturn(Mockito.mock(SSOPrincipal.class));
    Assert.assertNotNull(service.validateUserToken("z"));
    service.invalidateUserToken("z");
    Assert.assertNull(service.validateUserToken("z"));

  }


  @Test
  public void testValidateAppToken() throws Exception {
    ForTestSSOService service = Mockito.spy(new ForTestSSOService());
    service.setConfiguration(new Configuration()); //60 sec cache

    //invalid, unknown
    Assert.assertNull(service.validateAppToken("x", "c"));
    Mockito.verify(service).validateAppTokenWithSecurityService(Mockito.eq("x"), Mockito.eq("c"));

    //invalid, cached
    Mockito.reset(service);
    Assert.assertNull(service.validateAppToken("x", "c"));
    Mockito.verify(service, Mockito.never()).validateAppTokenWithSecurityService(Mockito.eq("x"), Mockito.eq("c"));

    //valid, unknown
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);
    Mockito.when(principal.getPrincipalId()).thenReturn("c");
    Mockito.when(service.validateAppTokenWithSecurityService(Mockito.eq("a"), Mockito.eq("c"))).thenReturn(principal);

    Assert.assertEquals(principal, service.validateAppToken("a", "c"));
    Mockito.verify(service).validateAppTokenWithSecurityService(Mockito.eq("a"), Mockito.eq("c"));

    //valid, cached
    Mockito.reset(service);
    Assert.assertEquals(principal, service.validateAppToken("a", "c"));
    Mockito.verify(service, Mockito.never()).validateAppTokenWithSecurityService(Mockito.eq("a"), Mockito.eq("c"));

    //valid, incorrect component ID
    Assert.assertNull(service.validateAppToken("x", "cc"));

    service.initializePrincipalCaches(1); //1 millisec cache

    //valid, unknown
    Mockito.when(service.validateAppTokenWithSecurityService(Mockito.eq("b"), Mockito.eq("c"))).thenReturn(principal);

    Assert.assertEquals(principal, service.validateAppToken("b", "c"));
    Mockito.verify(service).validateAppTokenWithSecurityService(Mockito.eq("b"), Mockito.eq("c"));

    // cache expired
    Thread.sleep(2);

    //valid, unknown
    Mockito.reset(service);
    Mockito.when(service.validateAppTokenWithSecurityService(Mockito.eq("b"), Mockito.eq("c"))).thenReturn(principal);
    Assert.assertEquals(principal, service.validateAppToken("b", "c"));
    Mockito.verify(service).validateAppTokenWithSecurityService(Mockito.eq("b"), Mockito.eq("c"));
  }

  @Test
  public void testInvalidateAppToken() throws Exception {
    ForTestSSOService service = Mockito.spy(new ForTestSSOService());
    service.setConfiguration(new Configuration()); // 60 sec cache

    // unknown
    service.invalidateAppToken("x");
    Assert.assertNull(service.validateAppToken("x", "c"));

    // valid, unknown
    Mockito
        .when(service.validateAppTokenWithSecurityService(Mockito.eq("y"), Mockito.eq("c")))
        .thenReturn(Mockito.mock(SSOPrincipal.class));
    service.invalidateAppToken("y");
    Assert.assertNull(service.validateAppToken("y", "c"));

    // valid, cached
    Mockito
        .when(service.validateAppTokenWithSecurityService(Mockito.eq("z"), Mockito.eq("c")))
        .thenReturn(Mockito.mock(SSOPrincipal.class));
    Assert.assertNotNull(service.validateAppToken("z", "c"));
    service.invalidateAppToken("z");
    Assert.assertNull(service.validateAppToken("z", "c"));

  }

}
