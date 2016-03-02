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
package com.streamsets.lib.security.util;

import org.junit.Assert;
import org.junit.Test;

import java.security.KeyPair;
import java.security.PublicKey;

public class TestDataSignature {

  @Test
  public void testSignerVerifier() throws Exception {
    KeyPair kp = DataSignature.get().generateKeyPair();

    byte[] data = "HelloHelloGoodBye".getBytes();

    DataSignature.Signer signer = DataSignature.get().getSigner(kp.getPrivate());
    byte [] signature = signer.sign(data);

    DataSignature.Verifier verifier = DataSignature.get().getVerifier(kp.getPublic());
    Assert.assertTrue(verifier.verify(data, signature));

    data[0] = (byte) 'h';
    Assert.assertFalse(verifier.verify(data, signature));
  }

  @Test
  public void testEncodeDecodePublicKey() throws Exception {
    KeyPair kp = DataSignature.get().generateKeyPair();

    byte[] data = "HelloHelloGoodBye".getBytes();

    DataSignature.Signer signer = DataSignature.get().getSigner(kp.getPrivate());
    byte [] signature = signer.sign(data);

    String encodedPK = DataSignature.get().encodePublicKey(kp.getPublic());

    PublicKey publicKey = DataSignature.get().decodePublicKey(encodedPK);

    DataSignature.Verifier verifier = DataSignature.get().getVerifier(publicKey);
    Assert.assertTrue(verifier.verify(data, signature));
  }

}
