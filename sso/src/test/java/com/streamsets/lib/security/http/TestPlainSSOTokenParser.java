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
import com.google.common.base.Joiner;
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

  protected String createTokenStr(SSOUserPrincipal principal) throws Exception {
    String info = encodeToken(principal);
    String version = createParser().getType();
    return version + SSOConstants.TOKEN_PART_SEPARATOR + info;
  }

  protected String encodeToken(SSOUserPrincipal principal) throws Exception {
    Map tokenJson = new HashMap();
    tokenJson.put(SSOUserPrincipalImpl.USER_ID, principal.getName());
    tokenJson.put(SSOUserPrincipalImpl.USER_NAME, principal.getUserFullName());
    tokenJson.put(SSOUserPrincipalImpl.ORG_ID, principal.getOrganization());
    tokenJson.put(SSOUserPrincipalImpl.ORG_NAME, principal.getOrganizationFullName());
    tokenJson.put(SSOUserPrincipalImpl.ROLES, Joiner.on(",").join(principal.getRoles()));
    tokenJson.put(SSOUserPrincipalImpl.USER_EMAIL, principal.getEmail());
    tokenJson.put(SSOUserPrincipalImpl.TOKEN_ID, principal.getTokenId());
    tokenJson.put(SSOUserPrincipalImpl.EXPIRES, Long.toString(principal.getExpires()));
    tokenJson.put(SSOUserPrincipalImpl.ISSUER_URL, principal.getIssuerUrl());
    return Base64.encodeBase64String(new ObjectMapper().writeValueAsString(tokenJson).getBytes());
  }

  @Test
  public void testValidToken() throws Exception {
    SSOTokenParser parser = createParser();
    SSOUserPrincipal principal = TestSSOUserPrincipalImpl.createToken();
    String tokenStr = createTokenStr(principal);
    SSOUserPrincipal got = parser.parse(tokenStr);
    Assert.assertNotNull(got);
    Assert.assertEquals(tokenStr, got.getTokenStr());
    Assert.assertEquals(principal.getTokenId(), got.getTokenId());
    Assert.assertEquals(principal.getName(), got.getName());
    Assert.assertEquals(principal.getUserFullName(), got.getUserFullName());
    Assert.assertEquals(principal.getOrganization(), got.getOrganization());
    Assert.assertEquals(principal.getRoles(), got.getRoles());
    Assert.assertEquals(principal.getExpires(), got.getExpires());
  }

}
