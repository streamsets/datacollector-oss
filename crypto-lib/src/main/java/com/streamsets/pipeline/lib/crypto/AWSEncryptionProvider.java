/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.lib.crypto;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.processor.crypto.EncryptionMode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class AWSEncryptionProvider implements EncryptionProvider {
  private EncryptionMode mode;
  private AwsCrypto crypto;
  private CryptoMaterialsManager cmManager;

  public static final class Builder {
    private EncryptionMode mode;
    private AwsCrypto crypto;
    private CryptoMaterialsManager cmManager;

    private Builder() {
    }

    public Builder withMode(EncryptionMode mode) {
      this.mode = mode;
      return this;
    }

    public Builder withCrypto(AwsCrypto crypto) {
      this.crypto = crypto;
      return this;
    }

    public Builder withCmManager(CryptoMaterialsManager cmManager) {
      this.cmManager = cmManager;
      return this;
    }

    public AWSEncryptionProvider build() {
      return new AWSEncryptionProvider(
          checkNotNull(mode, "EncryptionMode is a required parameter"),
          checkNotNull(crypto, "AwsCrypto is a required parameter"),
          checkNotNull(cmManager, "CryptoMaterialsManager is a required parameter")
      );
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static AWSCredentialsProvider getCredentialsProvider(
      CredentialValue accessKeyId,
      CredentialValue secretKey
  ) throws StageException {
    AWSCredentialsProvider credentialsProvider;
    if (accessKeyId != null && secretKey != null && !accessKeyId.get().isEmpty() && !secretKey.get().isEmpty()) {
      credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
          accessKeyId.get(),
          secretKey.get()
      ));
    } else {
      credentialsProvider = new DefaultAWSCredentialsProviderChain();
    }
    return credentialsProvider;
  }

  private AWSEncryptionProvider(EncryptionMode mode, AwsCrypto crypto, CryptoMaterialsManager cmManager) {
    this.mode = mode;
    this.crypto = crypto;
    this.cmManager = cmManager;
  }

  @Override
  public List<ConfigIssue> init(Stage.Context context) {
    return Collections.emptyList();
  }

  @Override
  public CryptoResult<byte[], ?> process(byte[] in, Map<String, String> context) {
    CryptoResult<byte[], ?> result;
    if (mode == EncryptionMode.ENCRYPT) {
      result = crypto.encryptData(cmManager, in, context);
    } else {
      result = crypto.decryptData(cmManager, in);
    }
    return result;
  }
}
