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

import org.apache.commons.codec.binary.Base64;

import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;

public class DataSignature {

  public interface Signer {

    byte[] sign(byte[] data) throws GeneralSecurityException;

  }

  public interface Verifier {

    boolean verify(byte[] data, byte[] signature) throws GeneralSecurityException;

  }

  private static final DataSignature DATA_SIGNATURE = new DataSignature();

  public static DataSignature get() {
    return DATA_SIGNATURE;
  }

  private DataSignature() {
  }

  public KeyPair generateKeyPair() throws GeneralSecurityException {
    KeyPairGenerator keyGenerator = KeyPairGenerator.getInstance("DSA");
    SecureRandom rng = SecureRandom.getInstance("SHA1PRNG", "SUN");
    rng.setSeed(System.currentTimeMillis());
    keyGenerator.initialize(1024, rng);
    return keyGenerator.generateKeyPair();
  }

  public String encodePublicKey(PublicKey publicKey) {
    return Base64.encodeBase64String(publicKey.getEncoded());
  }

  public PublicKey decodePublicKey(String encodedPublicKey) throws GeneralSecurityException {
    byte[] bytes = Base64.decodeBase64(encodedPublicKey);
    X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(bytes);
    KeyFactory keyFactory = KeyFactory.getInstance("DSA", "SUN");
    return keyFactory.generatePublic(pubKeySpec);
  }

  public Signer getSigner(final PrivateKey privateKey) {
    return new Signer() {
      @Override
      public byte[] sign(byte[] data) throws GeneralSecurityException {
        Signature signer = Signature.getInstance("SHA1withDSA");
        signer.initSign(privateKey);
        signer.update(data);
        return signer.sign();
      }
    };
  }

  public Verifier getVerifier(final PublicKey publicKey) {
    return new Verifier() {
      @Override
      public boolean verify(byte[] data, byte[] signature) throws GeneralSecurityException {
        Signature signer = Signature.getInstance("SHA1withDSA");
        signer.initVerify(publicKey);
        signer.update(data);
        return signer.verify(signature);
      }
    };
  }

}
