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

import java.security.GeneralSecurityException;
import java.security.KeyPair;

public class SignedSSOTokenGenerator extends PlainSSOTokenGenerator {
  private final long keyGenerationFrequencySecs;
  private volatile KeyPair signingKeys;
  private volatile long lastKeyGenerationTime;

  public SignedSSOTokenGenerator(long keyGenerationFrequencySecs) {
    this.keyGenerationFrequencySecs = keyGenerationFrequencySecs;
    generateKeysIfNecessary();
  }

  @Override
  public String getType() {
    return SignedSSOTokenParser.TYPE;
  }

  @Override
  public String getVerificationData() {
    generateKeysIfNecessary();
    return Base64.encodeBase64String(signingKeys.getPublic().getEncoded());
  }

  void generateKeys() {
    try {
      signingKeys = DataSignature.get().generateKeyPair();
    } catch (GeneralSecurityException ex) {
      throw new RuntimeException(ex);
    }
  }

  boolean isTimeToGenerateKeys() {
    return System.currentTimeMillis() - lastKeyGenerationTime > keyGenerationFrequencySecs * 1000;
  }

  void generateKeysIfNecessary() {
    if (isTimeToGenerateKeys()) {
      boolean generate;
      synchronized (this) {
        generate = isTimeToGenerateKeys();
        if (generate) {
          lastKeyGenerationTime = System.currentTimeMillis();
        }
      }
      if (generate) {
        generateKeys();
      }
    }

  }

  @Override
  protected String generateData(SSOUserPrincipal principal) {
    String data = super.generateData(principal);
    generateKeysIfNecessary();
    try {
      String signature =
          Base64.encodeBase64String(DataSignature.get().getSigner(signingKeys.getPrivate()).sign(data.getBytes()));
      return signature + SSOConstants.TOKEN_PART_SEPARATOR + data;
    } catch (GeneralSecurityException ex) {
      throw new RuntimeException(ex);
    }
  }

}
