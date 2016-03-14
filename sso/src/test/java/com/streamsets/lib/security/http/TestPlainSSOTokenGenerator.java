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

import org.junit.Assert;
import org.junit.Test;

public class TestPlainSSOTokenGenerator {

  public SSOTokenGenerator getGenerator() {
    return new PlainSSOTokenGenerator();
  }

  public SSOTokenParser getParser() {
    return new PlainSSOTokenParser();
  }

  @Test
  public void testGeneratorToParser() throws Exception {
    SSOUserPrincipal expected = TestSSOUserPrincipalJson.createPrincipal();
    SSOTokenGenerator generator = getGenerator();
    String verificationData = generator.getVerificationData();
    String str = generator.generate(expected);
    SSOTokenParser parser = getParser();
    parser.setVerificationData(verificationData);
    SSOUserPrincipal parsed = parser.parse(str);
    Assert.assertNotNull(parsed);
    Assert.assertEquals(expected.getTokenId(), parsed.getTokenId());
    Assert.assertEquals(expected.getExpires(), parsed.getExpires());
    Assert.assertEquals(expected.getIssuerUrl(), parsed.getIssuerUrl());
    Assert.assertEquals(expected.getName(), parsed.getName());
    Assert.assertEquals(expected.getOrganizationId(), parsed.getOrganizationId());
    Assert.assertEquals(expected.getPrincipalName(), parsed.getPrincipalName());
    Assert.assertEquals(expected.getOrganizationName(), parsed.getOrganizationName());
    Assert.assertEquals(expected.getEmail(), parsed.getEmail());
    Assert.assertEquals(expected.getRoles(), parsed.getRoles());
    Assert.assertEquals(expected.getAttributes(), parsed.getAttributes());
  }
}
