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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestSSOUserToken {

  public static final String TOKEN_ID = "TOKEN_ID";

  public static SSOUserToken createToken() {
    Map<String, Object> userInfo = new HashMap<>();
    userInfo.put(SSOUserToken.TOKEN_ID, TOKEN_ID);
    userInfo.put(SSOUserToken.EXPIRES, System.currentTimeMillis() + 1000 * 1000);
    userInfo.put(SSOUserToken.ISSUER_URL, "URL");
    userInfo.put(SSOUserToken.USER_ID, "USERID@ORGID");
    userInfo.put(SSOUserToken.USER_NAME, "NAME");
    userInfo.put(SSOUserToken.ORG_ID, "ORGID");
    userInfo.put(SSOUserToken.ROLES, ImmutableList.of("ROLE"));
    userInfo.put("foo", "bar");
    return new SSOUserToken(userInfo);
  }

  @Test
  public void testToken() {
    SSOUserToken token = createToken();
    Assert.assertEquals(TOKEN_ID, token.getId());
    Assert.assertEquals("USERID@ORGID", token.getUserId());
    Assert.assertEquals("URL", token.getIssuerUrl());
    Assert.assertEquals("NAME", token.getUserName());
    Assert.assertEquals("ORGID", token.getOrganizationId());
    Assert.assertEquals(ImmutableList.of("ROLE"), token.getRoles());
    Assert.assertEquals(ImmutableMap.of("foo", "bar"), token.getUserAttributes());
    Assert.assertTrue(System.currentTimeMillis() < token.getExpires());

    Assert.assertNull(token.getRawToken());
    token.setRawToken("raw");
    Assert.assertEquals("raw", token.getRawToken());
    try {
      token.setRawToken("foo");
      Assert.fail();
    } catch (IllegalStateException ex) {
      //NOP
    }
  }

}
