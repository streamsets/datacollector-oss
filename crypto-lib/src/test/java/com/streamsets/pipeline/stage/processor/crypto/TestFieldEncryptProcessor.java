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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestFieldEncryptProcessor {
  private static final int KEY_SIZE = 256 / 8;
  private static final byte[] RAW_KEY = new byte[KEY_SIZE];

  private static CredentialValue key;
  private static Map<String, String> aad;

  @BeforeClass
  public static void setUpClass() {
    Arrays.fill(RAW_KEY, (byte) 0);
    key = () -> Base64.getEncoder().encodeToString(RAW_KEY);
    aad = Maps.newHashMap(ImmutableMap.of("abc", "def"));
  }

  @Test
  public void testInit() throws Exception {
    ProcessorFieldEncryptConfig conf = new ProcessorFieldEncryptConfig();
    conf.mode = EncryptionMode.ENCRYPT;
    conf.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    conf.fieldPaths = ImmutableList.of("/message");
    conf.key = key;
    conf.keyId = "keyId";
    conf.context = aad;
    conf.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    Processor processor = new FieldEncryptProcessor(conf);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldEncryptDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertTrue(issues.isEmpty());
  }

  @Test
  public void testWrongInputType() throws Exception {
    ProcessorFieldEncryptConfig decryptConfig = new ProcessorFieldEncryptConfig();
    decryptConfig.mode = EncryptionMode.DECRYPT;
    decryptConfig.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    decryptConfig.fieldPaths = ImmutableList.of("/");
    decryptConfig.key = key;
    decryptConfig.keyId = "keyId";
    decryptConfig.context = aad;
    decryptConfig.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    Processor decryptProcessor = new FieldEncryptProcessor(decryptConfig);

    ProcessorRunner decryptRunner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        decryptProcessor
    ).addOutputLane("lane").build();

    Record record = RecordCreator.create();
    record.set(Field.create("abcdef"));

    decryptRunner.runInit();
    StageRunner.Output output = decryptRunner.runProcess(ImmutableList.of(record));
    List<Record> decryptedRecords = output.getRecords().get("lane");
    assertEquals(0, decryptedRecords.size());
    List<Record> errors = decryptRunner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals(record.get(), errors.get(0).get());
  }

  @Test
  public void testOutOfRangeConfigValue() throws Exception {
    ProcessorFieldEncryptConfig config = new ProcessorFieldEncryptConfig();
    config.mode = EncryptionMode.ENCRYPT;
    config.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    config.fieldPaths = ImmutableList.of("/");
    config.key = key;
    config.keyId = "keyId";
    config.context = aad;
    config.dataKeyCaching = true;
    config.maxKeyAge = 600;
    config.maxRecordsPerKey = 1000;
    config.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    Processor encryptProcessor = new FieldEncryptProcessor(config);

    ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertTrue(issues.isEmpty());

    // bytes < 1
    config.maxBytesPerKey = "0";
    encryptProcessor = new FieldEncryptProcessor(config);

    runner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();

    issues = runner.runValidateConfigs();
    assertTrue(issues.get(0).toString().contains("must be in the range"));

    // value is not an integer
    config.maxBytesPerKey = "abc";
    encryptProcessor = new FieldEncryptProcessor(config);

    runner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();

    issues = runner.runValidateConfigs();
    assertTrue(issues.get(0).toString().contains("not a valid integer"));
  }

  @Test
  public void testNonCacheableCipher() throws Exception {
    ProcessorFieldEncryptConfig config = new ProcessorFieldEncryptConfig();
    config.mode = EncryptionMode.ENCRYPT;
    config.cipher = CryptoAlgorithm.ALG_AES_128_GCM_IV12_TAG16_NO_KDF;
    config.fieldPaths = ImmutableList.of("/");
    config.key = key;
    config.keyId = "keyId";
    config.context = aad;
    config.dataKeyCaching = true;
    config.maxKeyAge = 600;
    config.maxRecordsPerKey = 1000;
    config.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    Processor encryptProcessor = new FieldEncryptProcessor(config);

    ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("Data key caching is not supported"));
  }

  @Test
  public void testProcess() throws Exception {
    final String message = "Hello, World!";
    final long longValue = 1234L;
    final boolean boolValue = true;

    ProcessorFieldEncryptConfig encryptConfig = new ProcessorFieldEncryptConfig();
    encryptConfig.mode = EncryptionMode.ENCRYPT;
    encryptConfig.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    encryptConfig.fieldPaths = ImmutableList.of("/message", "/long", "/bool", "/nonExistentField", "/nullValuedField");
    encryptConfig.key = key;
    encryptConfig.keyId = "keyId";
    encryptConfig.context = aad;
    encryptConfig.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    ProcessorFieldEncryptConfig decryptConfig = new ProcessorFieldEncryptConfig();
    decryptConfig.mode = EncryptionMode.DECRYPT;
    decryptConfig.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    decryptConfig.fieldPaths = ImmutableList.of("/message", "/long", "/bool");
    decryptConfig.key = key;
    decryptConfig.keyId = "keyId";
    decryptConfig.context = aad;
    decryptConfig.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    Processor encryptProcessor = new FieldEncryptProcessor(encryptConfig);

    ProcessorRunner encryptRunner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();

    Record record = RecordCreator.create();

    Field messageField = Field.create(message);
    Field longField = Field.create(longValue);
    Field boolField = Field.create(boolValue);
    Field rootField = Field.create(ImmutableMap.<String, Field>builder()
        .put("message", messageField)
        .put("long", longField)
        .put("bool", boolField)
        .put("nullValuedField", Field.create(Field.Type.STRING, null))
        .build());
    record.set(rootField);

    List<Record> records = Collections.singletonList(record);
    encryptRunner.runInit();
    StageRunner.Output output = encryptRunner.runProcess(records);
    List<Record> encryptedRecords = output.getRecords().get("lane");
    assertEquals(1, encryptedRecords.size());

    Processor decryptProcessor = new FieldEncryptProcessor(decryptConfig);

    ProcessorRunner decryptRunner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        decryptProcessor
    ).addOutputLane("lane").build();

    decryptRunner.runInit();
    output = decryptRunner.runProcess(encryptedRecords);
    List<Record> decryptedRecords = output.getRecords().get("lane");
    assertEquals(1, decryptedRecords.size());
    assertEquals(messageField, decryptedRecords.get(0).get("/message"));
    assertEquals(longField, decryptedRecords.get(0).get("/long"));
    assertEquals(boolField, decryptedRecords.get(0).get("/bool"));
    assertTrue(decryptedRecords.get(0).has("/nullValuedField"));
    assertNull(decryptedRecords.get(0).get("/nullValuedField").getValue());
  }
}
