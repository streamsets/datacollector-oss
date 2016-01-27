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
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestPlainSSOTokenParser {

  protected SSOTokenParser createParser() throws Exception {
    return new PlainSSOTokenParser();
  }

  @Test(expected = NullPointerException.class)
  public void testNullToken() throws Exception {
    createParser().parse(null);
  }

  @Test
  public void testInvalidToken() throws Exception {
    Assert.assertNull(createParser().parse(""));
  }

  @Test
  public void testInvalidTokenVersion() throws Exception {
    Assert.assertNull(createParser().parse("fooV" + SSOConstants.TOKEN_PART_SEPARATOR + "foo"));
  }

  @Test
  public void testInvalidTokenData() throws Exception {
    SSOTokenParser parser = createParser();
    String parserVersion = parser.getType();
    Assert.assertNull(parser.parse(parserVersion + SSOConstants.TOKEN_PART_SEPARATOR + "foo"));
  }

  protected String createTokenStr(SSOUserToken token) throws Exception {
    String info = encodeToken(token);
    String version = createParser().getType();
    return version + SSOConstants.TOKEN_PART_SEPARATOR + info;
  }

  protected String encodeToken(SSOUserToken token) throws Exception {
    Map tokenJson = new HashMap();
    tokenJson.put(SSOUserToken.USER_ID, token.getUserId());
    tokenJson.put(SSOUserToken.USER_NAME, token.getUserName());
    tokenJson.put(SSOUserToken.ORG_ID, token.getOrganizationId());
    tokenJson.put(SSOUserToken.ROLES, token.getRoles());
    tokenJson.put(SSOUserToken.TOKEN_ID, token.getId());
    tokenJson.put(SSOUserToken.EXPIRES, token.getExpires());
    tokenJson.put(SSOUserToken.ISSUER_URL, token.getIssuerUrl());
    return Base64.encodeBase64String(new ObjectMapper().writeValueAsString(tokenJson).getBytes());
  }

  @Test
  public void testValidToken() throws Exception {
    SSOTokenParser parser = createParser();
    SSOUserToken token = TestSSOUserToken.createToken();
    String tokenStr = createTokenStr(token);
    SSOUserToken got = parser.parse(tokenStr);
    Assert.assertNotNull(got);
    Assert.assertEquals(tokenStr, got.getRawToken());
    Assert.assertEquals(token.getId(), got.getId());
    Assert.assertEquals(token.getUserId(), got.getUserId());
    Assert.assertEquals(token.getUserName(), got.getUserName());
    Assert.assertEquals(token.getOrganizationId(), got.getOrganizationId());
    Assert.assertEquals(token.getRoles(), got.getRoles());
    Assert.assertEquals(token.getExpires(), got.getExpires());
  }

}
