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
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.crypto.AWSEncryptionProvider;
import com.streamsets.pipeline.lib.crypto.EncryptionProvider;
import com.streamsets.pipeline.lib.crypto.JceMasterKeyProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_01;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_02;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_03;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_04;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_05;
import static com.streamsets.pipeline.lib.crypto.CryptoErrors.CRYPTO_06;

public class FieldEncryptProcessor extends SingleLaneRecordProcessor {
  private static final String SDC_FIELD_TYPE = "SDC_FIELD_TYPE";
  private static final ImmutableSet<Field.Type> UNSUPPORTED_TYPES = ImmutableSet.of(Field.Type.MAP,
      Field.Type.LIST,
      Field.Type.LIST_MAP
  );

  private FieldEncryptConfig conf;
  private EncryptionProvider encryptionProvider;
  private BiFunction<Field, Map<String, String>, byte[]> prepare;
  private BiFunction<Record, Field, Optional<Field>> checkInput;
  private Function<CryptoResult<byte[], ?>, Field> createResultField;

  public FieldEncryptProcessor(FieldEncryptConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (Security.getProvider("BC") == null) {
      Security.addProvider(new BouncyCastleProvider());
    }

    if (conf.dataKeyCaching && !conf.cipher.isSafeToCache()) {
      issues.add(getContext().createConfigIssue(EncryptGroups.PROVIDER.name(), "conf.dataKeyCaching", CRYPTO_06));
    }

    encryptionProvider = createProvider(issues);

    if (!issues.isEmpty()) {
      return issues;
    }

    issues.addAll(encryptionProvider.init(getContext())); // NOSONAR false positive

    if (conf.mode == EncryptionMode.ENCRYPT) {
      prepare = this::prepareEncrypt;
      checkInput = this::checkInputEncrypt;
      createResultField = this::createResultFieldEncrypt;
    } else {
      prepare = (field, context) -> prepareDecrypt(field);
      checkInput = this::checkInputDecrypt;
      createResultField = this::createResultFieldDecrypt;
    }

    return issues;
  }

  private EncryptionProvider createProvider(List<ConfigIssue> issues) {
    CryptoMaterialsManager cmManager = createCryptoMaterialsManager(issues);

    if (!issues.isEmpty()) {
      return null;
    }

    AwsCrypto crypto = new AwsCrypto();
    crypto.setEncryptionAlgorithm(conf.cipher);
    crypto.setEncryptionFrameSize(conf.frameSize);

    return AWSEncryptionProvider.builder().withMode(conf.mode).withCrypto(crypto).withCmManager(cmManager).build();
  }

  private CryptoMaterialsManager createCryptoMaterialsManager(List<ConfigIssue> issues) {
    AWSCredentialsProvider credentialsProvider;
    try {
      credentialsProvider = AWSEncryptionProvider.getCredentialsProvider(conf.aws.accessKeyId, conf.aws.secretAccessKey);
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(
          EncryptGroups.PROVIDER.name(),
          "conf.aws.accessKeyId",
          CRYPTO_01,
          e.toString()
      ));
      return null;
    }

    MasterKeyProvider<?> keyProvider;
    if (conf.masterKeyProvider == MasterKeyProviders.AWS_KMS) {
      keyProvider = KmsMasterKeyProvider.builder().withKeysForEncryption(conf.aws.keyArn).withCredentials(
          credentialsProvider).build();
    } else {
      keyProvider = new JceMasterKeyProvider(Base64.getDecoder().decode(conf.key), conf.keyId);
    }

    CryptoMaterialsCache cache = new LocalCryptoMaterialsCache(conf.cacheCapacity);

    CryptoMaterialsManager cmManager = new DefaultCryptoMaterialsManager(keyProvider);

    long maxBytesPerKey;
    try {
      maxBytesPerKey = Long.parseLong(conf.maxBytesPerKey);
    } catch (NumberFormatException e) {
      issues.add(getContext().createConfigIssue(
          EncryptGroups.PROVIDER.name(),
          "conf.maxBytesPerKey",
          CRYPTO_04,
          conf.maxBytesPerKey
      ));
      return null;
    }

    if (maxBytesPerKey < 1) {
      issues.add(getContext().createConfigIssue(
          EncryptGroups.PROVIDER.name(),
          "conf.maxBytesPerKey",
          CRYPTO_05,
          conf.maxBytesPerKey,
          1L,
          Long.MAX_VALUE
      ));
      return null;
    }

    if (conf.dataKeyCaching) {
      cmManager = CachingCryptoMaterialsManager.newBuilder()
          .withMasterKeyProvider(keyProvider)
          .withCache(cache)
          .withMaxAge(conf.maxKeyAge, TimeUnit.SECONDS)
          .withMessageUseLimit(conf.maxRecordsPerKey)
          .withByteUseLimit(maxBytesPerKey)
          .build();
    }

    return cmManager;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    Map<String, String> encryptionContext = new HashMap<>(conf.context);

    for (String fieldPath : conf.fieldPaths) {

      Field field = record.get(fieldPath);

      // reviewer requested no use of Java 8 streams
      if (field != null && field.getValue() != null) { // process if field is present and non-null

        Optional<Field> input = checkInput.apply(record, field);

        if (input.isPresent()) {
          byte[] bytes = prepare.apply(input.get(), encryptionContext);
          CryptoResult<byte[], ?> result = encryptionProvider.process(bytes, encryptionContext);
          field = createResultField.apply(result);
          record.set(fieldPath, field);
        } else {
          return; // record sent to error, done with this record.
        }
      }
    }

    singleLaneBatchMaker.addRecord(record);
  }

  /**
   * Checks that the encryption input is a supported type, otherwise sends the
   * record to error.
   *
   * @param record {@link Record}
   * @param field {@link Field}
   * @return empty {@link Optional} if the record was sent to error
   */
  private Optional<Field> checkInputEncrypt(Record record, Field field) {
    if (UNSUPPORTED_TYPES.contains(field.getType())) {
      getContext().toError(record, CRYPTO_03, field.getType());
      return Optional.empty();
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
  private Optional<Field> checkInputDecrypt(Record record, Field field) {
    if (field.getType() != Field.Type.BYTE_ARRAY) {
      getContext().toError(record, CRYPTO_02, field.getType());
      return Optional.empty();
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
  private byte[] prepareEncrypt(Field field, Map<String, String> context) {
    context.put(SDC_FIELD_TYPE, field.getType().name());

    if (field.getType() == Field.Type.BYTE_ARRAY) {
      return field.getValueAsByteArray();
    } else {
      // Treat all other data as strings
      return field.getValueAsString().getBytes(Charsets.UTF_8);
    }
  }

  private byte[] prepareDecrypt(Field field) {
    return field.getValueAsByteArray();
  }

  /**
   * Returns a decrypted {@link Field} with its original type preserved when type information
   * has been preserved in the AAD.
   *
   * @param result of the decryption operation
   * @return {@link Field}
   */
  private Field createResultFieldDecrypt(CryptoResult<byte[], ?> result) {
    return Field.create(
        Field.Type.valueOf(result.getEncryptionContext().getOrDefault(SDC_FIELD_TYPE, Field.Type.BYTE_ARRAY.name())),
        new String(result.getResult(), Charsets.UTF_8)
    );
  }

  /**
   * Returns an encrypted {@link Field} as a byte array {@link Field}
   *
   * @param result result of the encryption operation
   * @return {@link Field}
   */
  private Field createResultFieldEncrypt(CryptoResult<byte[], ?> result) {
    return Field.create(result.getResult());
  }
}
