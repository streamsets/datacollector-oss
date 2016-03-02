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
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PublicKey;

public class SignedSSOTokenParser extends PlainSSOTokenParser {
  private static final Logger LOG = LoggerFactory.getLogger(SignedSSOTokenParser.class);

  public static final String TYPE = "signed";

  private String lastEncodedKey;
  private volatile DataSignature.Verifier[] verifiers = new DataSignature.Verifier[0];

  @Override
  protected Logger getLog() {
    return LOG;
  }

  protected synchronized void setPublicKey(String encodedKey) {
    if (encodedKey != null) {
      if (!encodedKey.equals(lastEncodedKey)) {
        lastEncodedKey = encodedKey;
        try {
          PublicKey publicKey = DataSignature.get().decodePublicKey(encodedKey);
          DataSignature.Verifier newVerifier = DataSignature.get().getVerifier(publicKey);
          int size = (verifiers.length < 2) ? verifiers.length + 1 : 2;
          getLog().debug("Got new signature, rotating verifiers");
          DataSignature.Verifier[] newVerifiers = new DataSignature.Verifier[size];
          newVerifiers[0] = newVerifier;
          if (size == 2) {
            newVerifiers[1] = verifiers[0];
          }
          verifiers = newVerifiers;
        } catch (GeneralSecurityException ex) {
          getLog().error("Error setting public key, disabling verifier: {}", ex.toString(), ex);
          lastEncodedKey = null;
          verifiers = new DataSignature.Verifier[0];
        }
      } else {
        getLog().debug("Got same signature, ignoring");
      }
    } else {
      getLog().debug("Parser disabled, public key set to NULL");
      lastEncodedKey = null;
      verifiers = new DataSignature.Verifier[0];
    }
  }

  @Override
  public String getType() {
    return TYPE;
  }

  protected boolean verifySignature(String data, String signatureB64) {
    boolean valid = false;
    DataSignature.Verifier[] currentVerifiers = this.verifiers;
    if (currentVerifiers.length > 0) {
      for (int i = 0; !valid && i < currentVerifiers.length; i++) {
        try {
          valid = currentVerifiers[i].verify(data.getBytes(), Base64.decodeBase64(signatureB64));
        } catch (GeneralSecurityException ex) {
          getLog().error("Error verifying signature: {}", ex.toString(), ex);
        }
      }
      if (!valid) {
        getLog().warn("Invalid signature for '{}'", data);
      }
    } else {
      getLog().error("There are no public keys, cannot verify signature");
    }
    return valid;
  }

  @Override
  public void setVerificationData(String data) {
    setPublicKey(data);
  }

  @Override
  public SSOUserPrincipal parsePrincipal(String tokenStr, String data) throws IOException {
    Utils.checkNotNull(data, "data");
    SSOUserPrincipal token = null;
    String signatureB64 = getHead(data);
    if (signatureB64 == null) {
      getLog().warn("Invalid signed token '{}', cannot get signature", data);
    } else {
      String signedData = getTail(data);
      if (signedData == null) {
        getLog().warn("Invalid signed token '{}', cannot get signed data", data);
      } else {
        if (verifySignature(signedData, signatureB64)) {
          token = super.parsePrincipal(tokenStr, signedData);
        }
      }
    }
    return token;
  }

}
