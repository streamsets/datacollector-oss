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

public class TestSSOUserPrincipalJson {

  public static SSOUserPrincipal createPrincipal() {
    return createPrincipal(System.currentTimeMillis() + 1000);
  }

  public static SSOUserPrincipal createPrincipal(long expires) {
    SSOUserPrincipalJson p = new SSOUserPrincipalJson();
    p.setTokenStr("tokenStr");
    p.setExpires(expires);
    p.setIssuerUrl("issuerUrl");
    p.setPrincipalId("userId");
    p.setPrincipalName("userName");
    p.setEmail("email");
    p.setOrganizationId("orgId");
    p.setOrganizationName("orgName");
    p.getRoles().add("r1");
    p.getAttributes().put("a", "A");
    return p;
  }

  @Test
  public void testValid() {
    SSOUserPrincipal principal =  createPrincipal(1);
    Assert.assertEquals("tokenStr", principal.getTokenStr());
    Assert.assertEquals(1L, principal.getExpires());
    Assert.assertEquals("issuerUrl", principal.getIssuerUrl());
    Assert.assertEquals("userId", principal.getPrincipalId());
    Assert.assertEquals("userId", principal.getName());
    Assert.assertEquals("userName", principal.getPrincipalName());
    Assert.assertEquals("orgId", principal.getOrganizationId());
    Assert.assertEquals("orgName", principal.getOrganizationName());
    Assert.assertEquals("email", principal.getEmail());
    Assert.assertEquals(ImmutableSet.of("r1"), principal.getRoles());
    Assert.assertEquals(ImmutableMap.of("a", "A"), principal.getAttributes());
  }

  @Test(expected = IllegalStateException.class)
  public void testLock() {
    SSOUserPrincipalJson p = new SSOUserPrincipalJson();
    p.setPrincipalId("id");
    p.lock();
    p.setPrincipalId("id1");
  }
}
