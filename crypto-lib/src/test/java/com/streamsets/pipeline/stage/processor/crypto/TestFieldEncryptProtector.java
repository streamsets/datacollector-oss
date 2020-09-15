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
import com.streamsets.pipeline.api.base.BaseFieldProcessor;
import com.streamsets.pipeline.api.impl.RecordBasedFieldBatch;
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

public class TestFieldEncryptProtector {
  private static final int KEY_SIZE = 256 / 8;
  private static final byte[] RAW_KEY = new byte[KEY_SIZE];

  private static String key;
  private static Map<String, String> aad;

  @BeforeClass
  public static void setUpClass() {
    Arrays.fill(RAW_KEY, (byte) 0);
    key = Base64.getEncoder().encodeToString(RAW_KEY);
    aad = Maps.newHashMap(ImmutableMap.of("abc", "def"));
  }

  @Test
  public void testInit() throws Exception {
    ProtectorFieldEncryptConfig conf = new ProtectorFieldEncryptConfig();
    conf.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    conf.key = key;
    conf.keyId = "keyId";
    conf.context = aad;
    conf.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    Processor processor = new EncryptFieldProtector();
    ((EncryptFieldProtector) processor).conf = conf;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldEncryptDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertTrue(issues.isEmpty());
  }

  @Test
  public void testOutOfRangeConfigValue() throws Exception {
    ProtectorFieldEncryptConfig config = new ProtectorFieldEncryptConfig();
    config.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    config.key = key;
    config.keyId = "keyId";
    config.context = aad;
    config.dataKeyCaching = true;
    config.maxKeyAge = 600;
    config.maxRecordsPerKey = 1000;
    config.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    Processor encryptProcessor = new EncryptFieldProtector();
    ((EncryptFieldProtector) encryptProcessor).conf = config;

    ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertTrue(issues.isEmpty());

    // bytes < 1
    config.maxBytesPerKey = "0";
    encryptProcessor = new EncryptFieldProtector();
    ((EncryptFieldProtector) encryptProcessor).conf = config;

    runner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();

    issues = runner.runValidateConfigs();
    assertTrue(issues.get(0).toString().contains("must be in the range"));

    // value is not an integer
    config.maxBytesPerKey = "abc";
    encryptProcessor = new EncryptFieldProtector();
    ((EncryptFieldProtector) encryptProcessor).conf = config;

    runner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();

    issues = runner.runValidateConfigs();
    assertTrue(issues.get(0).toString().contains("not a valid integer"));
  }

  @Test
  public void testNonCacheableCipher() throws Exception {
    ProtectorFieldEncryptConfig config = new ProtectorFieldEncryptConfig();
    config.cipher = CryptoAlgorithm.ALG_AES_128_GCM_IV12_TAG16_NO_KDF;
    config.key = key;
    config.keyId = "keyId";
    config.context = aad;
    config.dataKeyCaching = true;
    config.maxKeyAge = 600;
    config.maxRecordsPerKey = 1000;
    config.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    Processor encryptProcessor = new EncryptFieldProtector();
    ((EncryptFieldProtector) encryptProcessor).conf = config;

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

    ProtectorFieldEncryptConfig encryptConfig = new ProtectorFieldEncryptConfig();
    encryptConfig.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    encryptConfig.key = key;
    encryptConfig.keyId = "keyId";
    encryptConfig.context = aad;
    encryptConfig.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    ProcessorFieldEncryptConfig decryptConfig = new ProcessorFieldEncryptConfig();
    decryptConfig.mode = EncryptionMode.DECRYPT;
    decryptConfig.cipher = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
    decryptConfig.fieldPaths = ImmutableList.of("/message", "/long", "/bool");
    decryptConfig.key = () -> key;
    decryptConfig.keyId = "keyId";
    decryptConfig.context = aad;
    decryptConfig.maxBytesPerKey = String.valueOf(Long.MAX_VALUE);

    BaseFieldProcessor encryptProcessor = new EncryptFieldProtector();
    ((EncryptFieldProtector) encryptProcessor).conf = encryptConfig;

    ProcessorRunner encryptRunner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        encryptProcessor
    ).addOutputLane("lane").build();
    encryptRunner.runInit();

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

    Processor decryptProcessor = new FieldEncryptProcessor(decryptConfig);

    ProcessorRunner decryptRunner = new ProcessorRunner.Builder(
        FieldEncryptDProcessor.class,
        decryptProcessor
    ).addOutputLane("lane").build();

    decryptRunner.runInit();
    encryptProcessor.process(new RecordBasedFieldBatch(record, ImmutableList.of("/message", "/long", "/bool", "/nonExistentField", "/nullValuedField").iterator()));
    StageRunner.Output output = decryptRunner.runProcess(Collections.singletonList(record));
    List<Record> decryptedRecords = output.getRecords().get("lane");
    assertEquals(1, decryptedRecords.size());
    assertEquals(messageField, decryptedRecords.get(0).get("/message"));
    assertEquals(longField, decryptedRecords.get(0).get("/long"));
    assertEquals(boolField, decryptedRecords.get(0).get("/bool"));
    assertTrue(decryptedRecords.get(0).has("/nullValuedField"));
    assertNull(decryptedRecords.get(0).get("/nullValuedField").getValue());
  }
}
