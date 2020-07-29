/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.processor.crypto;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessorFieldEncryptConfig implements FieldEncryptConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      description = "Select whether to encrypt or decrypt fields",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ACTION"
  )
  @ValueChooserModel(EncryptionModeChooserValues.class)
  public EncryptionMode mode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Fields",
      description = "Fields to encrypt",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ACTION"
  )
  @FieldSelectorModel
  public List<String> fieldPaths;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Master Key Provider",
      description =
          "Selects the provider of the master key provider.",
      defaultValue = "USER",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  @ValueChooserModel(MasterKeyProviderChoices.class)
  public MasterKeyProviders masterKeyProvider;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Cipher",
      defaultValue = "ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  @ValueChooserModel(CryptoAlgorithmChoices.class)
  public CryptoAlgorithm cipher;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Frame Size",
      description = "Framing can be disabled by setting the frame size to 0.",
      defaultValue = "4096",
      min = 0,
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  public int frameSize = 4096;


  @ConfigDefBean(groups = "PROVIDER")
  public AmazonKMSConfig aws = new AmazonKMSConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Base64 Encoded Key",
      description = "Enter an encoded key, use a Base64 function to encode a string, or use a credential function to retrieve the key from a credential store.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "masterKeyProvider", triggeredByValues = {"USER"})
      },
      group = "PROVIDER"
  )
  public CredentialValue key;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Key ID (Optional)",
      description = "An optional identifier for the master key used to encrypt the data.",
      displayPosition = 41,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "masterKeyProvider", triggeredByValues = {"USER"})
      },
      group = "PROVIDER"
  )
  public String keyId;


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Encryption Context (AAD)",
      dependencies = {
          @Dependency(configName = "mode", triggeredByValues = {"ENCRYPT"})
      },
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  public Map<String, String> context = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Data Key Caching",
      defaultValue = "true",
      description = "Data key caching can improve performance, reduce cost, and help you stay within service limits.",
      dependencies = {
          @Dependency(configName = "mode", triggeredByValues = {"ENCRYPT"})
      },
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  public boolean dataKeyCaching;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Cache Capacity",
      defaultValue = "10",
      min = 1,
      description = "Maximum number of data keys to keep, regardless of TTL/usage settings.",
      dependencies = {
          @Dependency(configName = "dataKeyCaching", triggeredByValues = "true"),
          @Dependency(configName = "mode", triggeredByValues = {"ENCRYPT"})
      },
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  public int cacheCapacity;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Data Key Age",
      description = "Maximum duration to reuse data keys.",
      defaultValue = "60",
      min = 1,
      dependencies = {
          @Dependency(configName = "dataKeyCaching", triggeredByValues = "true"),
          @Dependency(configName = "mode", triggeredByValues = {"ENCRYPT"})
      },
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  public long maxKeyAge;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Records per Data Key",
      description = "Maximum number of records to encrypt with the same key.",
      defaultValue = "10000",
      min = 1,
      max = 4294967296L,
      dependencies = {
          @Dependency(configName = "dataKeyCaching", triggeredByValues = "true"),
          @Dependency(configName = "mode", triggeredByValues = {"ENCRYPT"})
      },
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  public long maxRecordsPerKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING, // Javascript does not support the full range of Long values
      label = "Max Bytes per Data Key",
      description = "Maximum number of bytes to encrypt with the same key.",
      defaultValue = "9223372036854775807", // Long.MAX_VALUE
      dependencies = {
          @Dependency(configName = "dataKeyCaching", triggeredByValues = "true"),
          @Dependency(configName = "mode", triggeredByValues = {"ENCRYPT"})
      },
      displayPosition = 150,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROVIDER"
  )
  public String maxBytesPerKey;

  public EncryptionMode getMode() {
    return mode;
  }

  public List<String> getFieldPaths() {
    return fieldPaths;
  }

  public MasterKeyProviders getMasterKeyProvider() {
    return masterKeyProvider;
  }

  public CryptoAlgorithm getCipher() {
    return cipher;
  }

  public int getFrameSize() {
    return frameSize;
  }

  public AmazonKMSConfig getAws() {
    return aws;
  }

  public String getKey() {
    return key.get();
  }

  public String getKeyId() {
    return keyId;
  }

  public Map<String, String> getContext() {
    return context;
  }

  public boolean isDataKeyCaching() {
    return dataKeyCaching;
  }

  public int getCacheCapacity() {
    return cacheCapacity;
  }

  public long getMaxKeyAge() {
    return maxKeyAge;
  }

  public long getMaxRecordsPerKey() {
    return maxRecordsPerKey;
  }

  public String getMaxBytesPerKey() {
    return maxBytesPerKey;
  }
}
