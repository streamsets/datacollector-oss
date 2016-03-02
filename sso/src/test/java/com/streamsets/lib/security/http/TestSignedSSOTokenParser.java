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

import com.streamsets.lib.security.util.DataSignature;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.security.KeyPair;

public class TestSignedSSOTokenParser extends TestPlainSSOTokenParser {
  private KeyPair keyPair;

  public TestSignedSSOTokenParser() throws Exception {
    spinKeyPair();
  }

  protected KeyPair getKeyPair() {
    return keyPair;
  }

  protected void spinKeyPair() throws Exception {
    keyPair = DataSignature.get().generateKeyPair();
  }

  @Override
  protected SSOTokenParser createParser() throws Exception {
    SignedSSOTokenParser parser = new SignedSSOTokenParser();
    parser.setVerificationData(DataSignature.get().encodePublicKey(getKeyPair().getPublic()));
    return parser;
  }

  @Override
  protected String createTokenStr(SSOUserPrincipal principal) throws Exception {
    String info = encodeToken(principal);
    String version = createParser().getType();
    String signature =
        Base64.encodeBase64String(DataSignature.get().getSigner(getKeyPair().getPrivate()).sign(info.getBytes()));
    return version + SSOConstants.TOKEN_PART_SEPARATOR + signature + SSOConstants.TOKEN_PART_SEPARATOR + info;
  }

  @Test
  public void testParserNoKey() throws Exception {
    SignedSSOTokenParser parser = new SignedSSOTokenParser();
    Assert.assertNull(parser.parsePrincipal("", ""));
  }

  @Test
  public void testTwoKeys() throws Exception {
    SignedSSOTokenParser parser = new SignedSSOTokenParser();
    parser.setVerificationData(DataSignature.get().encodePublicKey(getKeyPair().getPublic()));
    parser.setVerificationData(DataSignature.get().encodePublicKey(getKeyPair().getPublic()));
    String tokenWithFirstKey = createTokenStr(TestSSOUserPrincipalImpl.createToken());
    SSOUserPrincipal got = parser.parse(tokenWithFirstKey);
    Assert.assertNotNull(got);

    spinKeyPair();
    parser.setVerificationData(DataSignature.get().encodePublicKey(getKeyPair().getPublic()));
    got = parser.parse(tokenWithFirstKey);
    Assert.assertNotNull(got);
    String tokenWithSecondtKey = createTokenStr(TestSSOUserPrincipalImpl.createToken());
    got = parser.parse(tokenWithSecondtKey);
    Assert.assertNotNull(got);

    spinKeyPair();
    parser.setVerificationData(DataSignature.get().encodePublicKey(getKeyPair().getPublic()));
    got = parser.parse(tokenWithFirstKey);
    Assert.assertNull(got);
    got = parser.parse(tokenWithSecondtKey);
    Assert.assertNotNull(got);
    String tokenWithThirdKey = createTokenStr(TestSSOUserPrincipalImpl.createToken());
    got = parser.parse(tokenWithThirdKey);
    Assert.assertNotNull(got);
  }
}
