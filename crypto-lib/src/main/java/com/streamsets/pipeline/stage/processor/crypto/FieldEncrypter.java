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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.DefaultCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.MasterKeyProvider;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.CryptoMaterialsCache;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.crypto.AWSEncryptionProvider;
import com.streamsets.pipeline.lib.crypto.EncryptionProvider;
import com.streamsets.pipeline.lib.crypto.JceMasterKeyProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_01;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_02;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_03;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_04;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_05;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_06;

public class FieldEncrypter {
  private static final String SDC_FIELD_TYPE = "SDC_FIELD_TYPE";
  private static final ImmutableSet<Field.Type> UNSUPPORTED_TYPES = ImmutableSet.of(Field.Type.MAP,
      Field.Type.LIST,
      Field.Type.LIST_MAP
  );

  private final FieldEncryptConfig conf;
  private final EncryptionMode mode;
  private Processor.Context context;
  private EncryptionProvider encryptionProvider;

  public FieldEncrypter(FieldEncryptConfig conf, EncryptionMode mode) {
    this.conf = conf;
    this.mode = mode;
  }

  public List<ConfigIssue> init(List<ConfigIssue> issues) {
    if (Security.getProvider("BC") == null) {
      Security.addProvider(new BouncyCastleProvider());
    }

    if (conf.isDataKeyCaching() && !conf.getCipher().isSafeToCache()) {
      issues.add(getContext().createConfigIssue(ProcessorEncryptGroups.PROVIDER.name(), "conf.dataKeyCaching", CRYPTO_06));
    }

    encryptionProvider = createProvider(issues);

    if (!issues.isEmpty()) {
      return issues;
    }

    issues.addAll(encryptionProvider.init(getContext())); // NOSONAR false positive

    return issues;
  }

  public CryptoResult<byte[], ?> process(byte[] bytes, Map<String, String> encryptionContext) {
    return encryptionProvider.process(bytes, encryptionContext);
  }

  private EncryptionProvider createProvider(List<ConfigIssue> issues) {
    CryptoMaterialsManager cmManager = createCryptoMaterialsManager(issues);

    if (!issues.isEmpty()) {
      return null;
    }

    AwsCrypto crypto = new AwsCrypto();
    crypto.setEncryptionAlgorithm(conf.getCipher());
    crypto.setEncryptionFrameSize(conf.getFrameSize());

    return AWSEncryptionProvider.builder().withMode(mode).withCrypto(crypto).withCmManager(cmManager).build();
  }

  private CryptoMaterialsManager createCryptoMaterialsManager(List<ConfigIssue> issues) {
    AWSCredentialsProvider credentialsProvider;
    try {
      credentialsProvider = AWSEncryptionProvider.getCredentialsProvider(conf.getAws().accessKeyId, conf.getAws().secretAccessKey);
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(
          ProcessorEncryptGroups.PROVIDER.name(),
          "conf.aws.accessKeyId",
          CRYPTO_01,
          e.toString()
      ));
      return null;
    }

    MasterKeyProvider<?> keyProvider;
    if (conf.getMasterKeyProvider() == MasterKeyProviders.AWS_KMS) {
      keyProvider = KmsMasterKeyProvider.builder().withKeysForEncryption(conf.getAws().keyArn).withCredentials(
          credentialsProvider).build();
    } else {
      keyProvider = new JceMasterKeyProvider(Base64.getDecoder().decode(conf.getKey()), conf.getKeyId());
    }

    CryptoMaterialsCache cache = new LocalCryptoMaterialsCache(conf.getCacheCapacity());

    CryptoMaterialsManager cmManager = new DefaultCryptoMaterialsManager(keyProvider);

    long maxBytesPerKey;
    try {
      maxBytesPerKey = Long.parseLong(conf.getMaxBytesPerKey());
    } catch (NumberFormatException e) {
      issues.add(getContext().createConfigIssue(
          ProcessorEncryptGroups.PROVIDER.name(),
          "conf.maxBytesPerKey",
          CRYPTO_04,
          conf.getMaxBytesPerKey()
      ));
      return null;
    }

    if (maxBytesPerKey < 1) {
      issues.add(getContext().createConfigIssue(
          ProcessorEncryptGroups.PROVIDER.name(),
          "conf.maxBytesPerKey",
          CRYPTO_05,
          conf.getMaxBytesPerKey(),
          1L,
          Long.MAX_VALUE
      ));
      return null;
    }

    if (conf.isDataKeyCaching()) {
      cmManager = CachingCryptoMaterialsManager.newBuilder()
          .withMasterKeyProvider(keyProvider)
          .withCache(cache)
          .withMaxAge(conf.getMaxKeyAge(), TimeUnit.SECONDS)
          .withMessageUseLimit(conf.getMaxRecordsPerKey())
          .withByteUseLimit(maxBytesPerKey)
          .build();
    }

    return cmManager;
  }

  public Processor.Context getContext() {
    return context;
  }

  public void setContext(Processor.Context context) {
    this.context = context;
  }

  /**
   * Checks that the encryption input is a supported type, otherwise sends the
   * record to error.
   *
   * @param record {@link Record}
   * @param field {@link Field}
   * @return empty {@link Optional} if the record was sent to error
   */
  public Optional<Field> checkInputEncrypt(Record record, Field field) {
    if (UNSUPPORTED_TYPES.contains(field.getType())) {
      getContext().toError(record, CRYPTO_03, field.getType());
      return Optional.empty();
    }
    return Optional.of(field);
  }

  /**
   * Checks that the encryption input is a supported type, otherwise sends the
   * record to StageException.
   *
   * @param field {@link Field}
   * @return empty {@link Optional} if the record was sent to error
   */
  public Optional<Field> checkInputEncrypt(Field field) throws StageException {
    if (UNSUPPORTED_TYPES.contains(field.getType())) {
      throw new StageException(CRYPTO_03, field.getType());
    }
    return Optional.of(field);
  }


  /**
   * Checks that the decryption input is a valid type, otherwise sends the
   * record to error.
   *
   * @param record {@link Record}
   * @param field {@link Field}
   * @return empty {@link Optional} if the record was sent to error
   */
  public Optional<Field> checkInputDecrypt(Record record, Field field) {
    if (field.getType() != Field.Type.BYTE_ARRAY) {
      getContext().toError(record, CRYPTO_02, field.getType());
      return Optional.empty();
    }
    return Optional.of(field);
  }

  /**
   * Checks that the decryption input is a valid type, otherwise sends the
   * record to StageException.
   *
   * @param field {@link Field}
   * @return empty {@link Optional} if the record was sent to error
   */
  public Optional<Field> checkInputDecrypt(Field field) throws StageException {
    if (field.getType() != Field.Type.BYTE_ARRAY) {
      throw new StageException(CRYPTO_02, field.getType());
    }
    return Optional.of(field);
  }

  /**
   * Does data type conversions in preparation for encryption.
   *
   * @param field {@link Field} to encrypt
   * @param context (AAD)
   * @return byte array to encrypt
   */
  public byte[] prepareEncrypt(Field field, Map<String, String> context) {
    context.put(SDC_FIELD_TYPE, field.getType().name());

    if (field.getType() == Field.Type.BYTE_ARRAY) {
      return field.getValueAsByteArray();
    } else {
      // Treat all other data as strings
      return field.getValueAsString().getBytes(Charsets.UTF_8);
    }
  }

  public byte[] prepareDecrypt(Field field) {
    return field.getValueAsByteArray();
  }

  /**
   * Returns a decrypted {@link Field} with its original type preserved when type information
   * has been preserved in the AAD.
   *
   * @param result of the decryption operation
   * @return {@link Field}
   */
  public Field createResultFieldDecrypt(CryptoResult<byte[], ?> result) {
    Field.Type fieldType = Field.Type.valueOf(
        result.getEncryptionContext().getOrDefault(SDC_FIELD_TYPE, Field.Type.BYTE_ARRAY.name())
    );

    // Field API prohibits STRING to BYTE_ARRAY conversion so this is a special case
    if (fieldType == Field.Type.BYTE_ARRAY) {
      return Field.create(result.getResult());
    }

    // Field API supports STRING to other primitive types.
    return Field.create(
        fieldType,
        new String(result.getResult())
    );
  }

  /**
   * Returns an encrypted {@link Field} as a byte array {@link Field}
   *
   * @param result result of the encryption operation
   * @return {@link Field}
   */
  public Field createResultFieldEncrypt(CryptoResult<byte[], ?> result) {
    return Field.create(result.getResult());
  }
}
