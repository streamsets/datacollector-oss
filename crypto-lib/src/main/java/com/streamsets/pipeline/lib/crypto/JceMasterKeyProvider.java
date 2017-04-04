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

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.DataKey;
import com.amazonaws.encryptionsdk.EncryptedDataKey;
import com.amazonaws.encryptionsdk.MasterKeyProvider;
import com.amazonaws.encryptionsdk.MasterKeyRequest;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.exception.NoSuchMasterKeyException;
import com.amazonaws.encryptionsdk.exception.UnsupportedProviderException;
import com.amazonaws.encryptionsdk.jce.JceMasterKey;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JceMasterKeyProvider extends MasterKeyProvider<JceMasterKey> {
  private static final String PROVIDER_NAME = "streamsets-user";

  private final JceMasterKey masterKey;

  public JceMasterKeyProvider(byte[] masterKeyData, String keyId) {
    SecretKey cryptoKey = new SecretKeySpec(masterKeyData, "AES");
    masterKey = JceMasterKey.getInstance(
        cryptoKey,
        "JceMasterKeyProvider",
        keyId,
        "AES/GCM/NoPadding"
    );
  }

  @Override
  public String getDefaultProviderId() {
    return PROVIDER_NAME;
  }

  @Override
  public JceMasterKey getMasterKey(String provider, String keyId) throws
      UnsupportedProviderException,
      NoSuchMasterKeyException {
    return masterKey.getMasterKey(provider, keyId);
  }

  @Override
  public List<JceMasterKey> getMasterKeysForEncryption(MasterKeyRequest request) {
    return Collections.singletonList(masterKey);
  }

  @Override
  public DataKey<JceMasterKey> decryptDataKey(
      CryptoAlgorithm algorithm,
      Collection<? extends EncryptedDataKey> encryptedDataKeys,
      Map<String, String> encryptionContext
  ) throws UnsupportedProviderException, AwsCryptoException {
    final List<Exception> exceptions = new ArrayList<>();
    for (final EncryptedDataKey edk : encryptedDataKeys) {
      try {
        final DataKey<JceMasterKey> result = masterKey.decryptDataKey(
            algorithm,
            Collections.singletonList(edk),
            encryptionContext);
        if (result != null) {
          return result;
        }
      } catch (final Exception ex) {
        exceptions.add(ex);
      }
    }

    throw buildCannotDecryptDksException(exceptions);
  }
}
