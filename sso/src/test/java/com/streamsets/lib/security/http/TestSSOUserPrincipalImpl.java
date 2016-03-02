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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestSSOUserPrincipalImpl {

  public static final String TOKEN_ID = "TOKEN_ID";

  public static Map<String, Object> createPayload() {
    Map<String, Object> userInfo = new HashMap<>();
    userInfo.put(SSOUserPrincipalImpl.TOKEN_ID, TOKEN_ID);
    userInfo.put(SSOUserPrincipalImpl.EXPIRES, Long.toString(System.currentTimeMillis() + 1000 * 1000));
    userInfo.put(SSOUserPrincipalImpl.ISSUER_URL, "URL");
    userInfo.put(SSOUserPrincipalImpl.USER_ID, "USERID@ORGID");
    userInfo.put(SSOUserPrincipalImpl.USER_NAME, "NAME");
    userInfo.put(SSOUserPrincipalImpl.ORG_ID, "ORGID");
    userInfo.put(SSOUserPrincipalImpl.ORG_NAME, "ORGNAME");
    userInfo.put(SSOUserPrincipalImpl.USER_EMAIL, "EMAIL");
    userInfo.put(SSOUserPrincipalImpl.ROLES, "ROLE1, ROLE2");
    userInfo.put("foo", "bar");
    return userInfo;
  }

  public static SSOUserPrincipal createToken() {
    return SSOUserPrincipalImpl.fromMap("", createPayload());
  }

  @Test
  public void testValid() {
    SSOUserPrincipal principal =  SSOUserPrincipalImpl.fromMap("raw", createPayload());
    Assert.assertEquals(TOKEN_ID, principal.getTokenId());
    Assert.assertEquals("USERID@ORGID", principal.getName());
    Assert.assertEquals("USERID@ORGID", principal.getUser());
    Assert.assertEquals("URL", principal.getIssuerUrl());
    Assert.assertEquals("NAME", principal.getUserFullName());
    Assert.assertEquals("ORGID", principal.getOrganization());
    Assert.assertEquals("ORGNAME", principal.getOrganizationFullName());
    Assert.assertEquals("EMAIL", principal.getEmail());
    Assert.assertEquals(ImmutableSet.of("ROLE1", "ROLE2"), principal.getRoles());
    Assert.assertEquals(ImmutableMap.of("foo", "bar"), principal.getAttributes());
    Assert.assertTrue(System.currentTimeMillis() < principal.getExpires());
    Assert.assertEquals("raw", principal.getTokenStr());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidValue() {
    Map<String, Object> map = createPayload();
    map.put("invadidType", 1);
    SSOUserPrincipalImpl.fromMap("raw", map);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingRequiredProperty() {
    Map<String, Object> map = createPayload();
    map.remove(SSOUserPrincipalImpl.ORG_ID);
    SSOUserPrincipalImpl.fromMap("raw", map);
  }

}
