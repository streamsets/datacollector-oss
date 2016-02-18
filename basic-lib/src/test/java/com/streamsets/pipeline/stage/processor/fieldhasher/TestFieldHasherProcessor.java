/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.PrimitiveSink;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.BooleanTypeSupport;
import com.streamsets.pipeline.api.impl.ByteArrayTypeSupport;
import com.streamsets.pipeline.api.impl.ByteTypeSupport;
import com.streamsets.pipeline.api.impl.CharTypeSupport;
import com.streamsets.pipeline.api.impl.DateTypeSupport;
import com.streamsets.pipeline.api.impl.DoubleTypeSupport;
import com.streamsets.pipeline.api.impl.FloatTypeSupport;
import com.streamsets.pipeline.api.impl.IntegerTypeSupport;
import com.streamsets.pipeline.api.impl.LongTypeSupport;
import com.streamsets.pipeline.api.impl.ShortTypeSupport;
import com.streamsets.pipeline.api.impl.StringTypeSupport;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestFieldHasherProcessor {

  private String hashForRecordsWithFieldsHeader(
      Record record,
      Collection<String> fieldsToHash,
      HashType hashType,
      boolean includeRecordHeaderForHashing
  ) {
    HashFunction hasher = HashingUtil.getHasher(hashType.getDigest());
    HashingUtil.RecordFunnel recordFunnel =
        HashingUtil.getRecordFunnel(
            fieldsToHash,
            includeRecordHeaderForHashing
        );
    return hasher.hashObject(record, recordFunnel).toString();
  }

  private String computeHashForRecordUsingFields(
      Record record,
      Collection<String> fieldsToHash,
      HashType hashType
  ) {
    return hashForRecordsWithFieldsHeader(
        record,
        fieldsToHash,
        hashType,
        false);
  }

  private String computeHash(Field.Type fieldType, Object value, HashType hashType) {
    HashFunction hasher = HashingUtil.getHasher(hashType.getDigest());
    PrimitiveSink sink = hasher.newHasher();
    switch (fieldType) {
      case BOOLEAN:
        sink.putBoolean(new BooleanTypeSupport().convert(value));
        break;
      case CHAR:
        sink.putChar(new CharTypeSupport().convert(value));
        break;
      case BYTE:
        sink.putByte(new ByteTypeSupport().convert(value));
        break;
      case SHORT:
        sink.putShort(new ShortTypeSupport().convert(value));
        break;
      case INTEGER:
        sink.putInt(new IntegerTypeSupport().convert(value));
        break;
      case LONG:
        sink.putLong(new LongTypeSupport().convert(value));
        break;
      case FLOAT:
        sink.putFloat(new FloatTypeSupport().convert(value));
        break;
      case DOUBLE:
        sink.putDouble(new DoubleTypeSupport().convert(value));
        break;
      case DATE:
        sink.putLong(new DateTypeSupport().convert(value).getTime());
        break;
      case DATETIME:
        sink.putLong(new DateTypeSupport().convert(value).getTime());
        break;
      case DECIMAL:
        sink.putString(new StringTypeSupport().convert(value), Charset.defaultCharset());
        break;
      case STRING:
        sink.putString(new StringTypeSupport().convert(value), Charset.defaultCharset());
        break;
      case BYTE_ARRAY:
        sink.putBytes(new ByteArrayTypeSupport().convert(value));
        break;
      case MAP:
      case LIST:
      default:
        return null;
    }
    sink.putByte((byte)0);
    return ((Hasher)sink).hash().toString();
  }

  private void populateEmptyRecordHasherConfig(HasherConfig hasherConfig) {
    hasherConfig.recordHasherConfig = new RecordHasherConfig();
    hasherConfig.recordHasherConfig.hashEntireRecord = false;
    hasherConfig.recordHasherConfig.headerAttribute = "";
    hasherConfig.recordHasherConfig.targetField = "";
    hasherConfig.recordHasherConfig.hashType = HashType.MD5;
  }

  private HasherConfig createInPlaceHasherProcessor(
      List<String> sourceFieldsToHash,
      HashType hashType
  ) {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.sourceFieldsToHash = sourceFieldsToHash;
    fieldHasherConfig.hashType = hashType;

    HasherConfig hasherConfig = new HasherConfig();
    populateEmptyRecordHasherConfig(hasherConfig);

    hasherConfig.inPlaceFieldHasherConfigs = ImmutableList.of(fieldHasherConfig);
    hasherConfig.targetFieldHasherConfigs =  Collections.EMPTY_LIST;
    return hasherConfig;
  }

  @Test
  public void testStringFieldWithContinue() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/name"), HashType.SHA2);
    FieldHasherProcessor processor =
        new FieldHasherProcessor(
            hasherConfig,
            OnStagePreConditionFailure.CONTINUE
        );

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      //No error records
      Assert.assertEquals(0, runner.getErrorRecords().size());

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "streamsets", HashType.SHA2),
          result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringFieldWithToError() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/name"), HashType.SHA2);
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      //No error records
      Assert.assertEquals(0, runner.getErrorRecords().size());

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "streamsets", HashType.SHA2),
          result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDecimalField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DECIMAL, new BigDecimal(345.678)));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(
          computeHash(Field.Type.DECIMAL, new BigDecimal(345.678), HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testIntegerField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.INTEGER, -123));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.INTEGER, "-123", HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testShortField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.SHORT, 123));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.SHORT, "123", HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testLongField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.LONG, 21474836478L));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.LONG, "21474836478", HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testByteField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.BYTE, 125));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.BYTE, "125", HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCharField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.CHAR, 'c'));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.CHAR, "c", HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testBooleanField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.BOOLEAN, true));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.BOOLEAN, "true", HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDateField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DATE, new Date(123456789)));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(
          computeHash(Field.Type.DATE, new Date(123456789), HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDateTimeField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DATETIME, new Date(123456789)));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(
          computeHash(Field.Type.DATETIME, new Date(123456789), HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFloatField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.FLOAT, 2.3842e-07f));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(
          computeHash(Field.Type.FLOAT, String.valueOf(2.3842e-07f), HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDoubleField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DOUBLE, 12342.3842));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.DOUBLE, 12342.3842, HashType.SHA2),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testByteArrayField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/byteArray"), HashType.SHA2);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("byteArray", Field.create(Field.Type.BYTE_ARRAY, "streamsets".getBytes()));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("byteArray"));
      Assert.assertEquals(
          computeHash(Field.Type.BYTE_ARRAY, "streamsets".getBytes(), HashType.SHA2),
          result.get("byteArray").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleFields() throws StageException {
    HasherConfig hasherConfig =
        createInPlaceHasherProcessor(
            ImmutableList.of("/age", "/name", "/sex", "/streetAddress"),
            HashType.SHA2
        );

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("sex", Field.create(Field.Type.STRING, null));
      map.put("streetAddress", Field.create("c"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "a", HashType.SHA2),
          result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.INTEGER, 21, HashType.SHA2),
          result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(null, result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "c", HashType.SHA2),
          result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldsWithNullValuesError() throws StageException {
    HasherConfig hasherConfig =
        createInPlaceHasherProcessor(
            ImmutableList.of("/age", "/name", "/sex", "/streetAddress"),
            HashType.SHA2
        );
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.TO_ERROR);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("sex", Field.create(Field.Type.STRING, null));
      map.put("streetAddress", Field.create("c"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());

      Field field = runner.getErrorRecords().get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "a", HashType.SHA2),
          result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.INTEGER, 21, HashType.SHA2),
          result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(null, result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "c", HashType.SHA2),
          result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleFieldsDifferentHashTypes() throws StageException {
    FieldHasherConfig sha1HasherConfig = new FieldHasherConfig();
    sha1HasherConfig.sourceFieldsToHash = ImmutableList.of("/age", "/name");
    sha1HasherConfig.hashType = HashType.SHA1;

    FieldHasherConfig sha2HasherConfig = new FieldHasherConfig();
    sha2HasherConfig.sourceFieldsToHash = ImmutableList.of("/sex");
    sha2HasherConfig.hashType = HashType.SHA2;

    FieldHasherConfig md5HasherConfig = new FieldHasherConfig();
    md5HasherConfig.sourceFieldsToHash = ImmutableList.of("/streetAddress");
    md5HasherConfig.hashType = HashType.MD5;

    HasherConfig hasherConfig = new HasherConfig();
    populateEmptyRecordHasherConfig(hasherConfig);

    hasherConfig.inPlaceFieldHasherConfigs = ImmutableList.of(sha1HasherConfig, sha2HasherConfig, md5HasherConfig);
    hasherConfig.targetFieldHasherConfigs =  Collections.EMPTY_LIST;

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("sex", Field.create(Field.Type.STRING, "male"));
      map.put("streetAddress", Field.create("sansome street"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "a", HashType.SHA1),
          result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.INTEGER, 21, HashType.SHA1),
          result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "male", HashType.SHA2),
          result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "sansome street", HashType.MD5),
          result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonExistingField() throws StageException {
    HasherConfig hasherConfig =
        createInPlaceHasherProcessor(
            ImmutableList.of("/nonExisting"),
            HashType.SHA2
        );
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("streamsets", result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonExistingFieldError() throws StageException {
    HasherConfig hasherConfig =
        createInPlaceHasherProcessor(
            ImmutableList.of("/nonExisting"),
            HashType.SHA2
        );
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.TO_ERROR);


    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Field field = runner.getErrorRecords().get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("streamsets", result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testUnsupportedFieldTypesContinue() throws StageException {
    HasherConfig hasherConfig =
        createInPlaceHasherProcessor(
            ImmutableList.of("/name", "/mapField", "/listField"),
            HashType.SHA2
        );

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));

      Map<String, Field> mapField = new HashMap<>();
      mapField.put("e1", Field.create("e1"));
      mapField.put("e2", Field.create("e2"));

      map.put("mapField", Field.create(mapField));
      map.put("listField", Field.create(Field.Type.LIST, ImmutableList.of(Field.create("e1"), Field.create("e2"))));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "streamsets", HashType.SHA2),
          result.get("name").getValue());

      Assert.assertTrue(result.containsKey("mapField"));
      Map<String, Field> m =  result.get("mapField").getValueAsMap();
      Assert.assertEquals("e1", m.get("e1").getValueAsString());
      Assert.assertEquals("e2", m.get("e2").getValueAsString());

      Assert.assertTrue(result.containsKey("listField"));
      List<Field> l = result.get("listField").getValueAsList();
      Assert.assertEquals("e1", l.get(0).getValueAsString());
      Assert.assertEquals("e2", l.get(1).getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testUnsupportedFieldTypesError() throws StageException {
    HasherConfig hasherConfig =
        createInPlaceHasherProcessor(
            ImmutableList.of("/name", "/mapField", "/listField"),
            HashType.SHA2
        );
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.TO_ERROR);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));

      Map<String, Field> mapField = new HashMap<>();
      mapField.put("e1", Field.create("e1"));
      mapField.put("e2", Field.create("e2"));

      map.put("mapField", Field.create(mapField));
      map.put("listField", Field.create(Field.Type.LIST, ImmutableList.of(Field.create("e1"), Field.create("e2"))));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());

      Assert.assertEquals(Errors.HASH_01.toString(), runner.getErrorRecords().get(0).getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardHashing() throws StageException {

    Field name1 = Field.create("jon");
    Field name2 = Field.create("natty");
    Map<String, Field> nameMap1 = new HashMap<>();
    nameMap1.put("name", name1);
    Map<String, Field> nameMap2 = new HashMap<>();
    nameMap2.put("name", name2);

    Field name3 = Field.create("adam");
    Field name4 = Field.create("hari");
    Map<String, Field> nameMap3 = new HashMap<>();
    nameMap3.put("name", name3);
    Map<String, Field> nameMap4 = new HashMap<>();
    nameMap4.put("name", name4);

    Field name5 = Field.create("madhu");
    Field name6 = Field.create("girish");
    Map<String, Field> nameMap5 = new HashMap<>();
    nameMap5.put("name", name5);
    Map<String, Field> nameMap6 = new HashMap<>();
    nameMap6.put("name", name6);

    Field first = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap1), Field.create(nameMap2)));
    Field second = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap3), Field.create(nameMap4)));
    Field third = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap5), Field.create(nameMap6)));

    Map<String, Field> noe = new HashMap<>();
    noe.put("streets", Field.create(ImmutableList.of(first, second)));

    Map<String, Field> cole = new HashMap<>();
    cole.put("streets", Field.create(ImmutableList.of(third)));


    Map<String, Field> sfArea = new HashMap<>();
    sfArea.put("noe", Field.create(noe));

    Map<String, Field> utahArea = new HashMap<>();
    utahArea.put("cole", Field.create(cole));


    Map<String, Field> california = new HashMap<>();
    california.put("SanFrancisco", Field.create(sfArea));

    Map<String, Field> utah = new HashMap<>();
    utah.put("SantaMonica", Field.create(utahArea));

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("USA", Field.create(Field.Type.LIST,
        ImmutableList.of(Field.create(california), Field.create(utah))));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "jon");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "natty");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "adam");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "hari");
    Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "madhu");
    Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), "girish");

    /* All the field Paths in the record are
        /USA
        /USA[0]
        /USA[0]/SantaMonica
        /USA[0]/SantaMonica/noe
        /USA[0]/SantaMonica/noe/streets
        /USA[0]/SantaMonica/noe/streets[0]
        /USA[0]/SantaMonica/noe/streets[0][0]
        /USA[0]/SantaMonica/noe/streets[0][0]/name
        /USA[0]/SantaMonica/noe/streets[0][1]
        /USA[0]/SantaMonica/noe/streets[0][1]/name
        /USA[0]/SantaMonica/noe/streets[1]
        /USA[0]/SantaMonica/noe/streets[1][0]
        /USA[0]/SantaMonica/noe/streets[1][0]/name
        /USA[0]/SantaMonica/noe/streets[1][1]
        /USA[0]/SantaMonica/noe/streets[1][1]/name
        /USA[1]
        /USA[1]/SantaMonica
        /USA[1]/SantaMonica/cole
        /USA[1]/SantaMonica/cole/streets
        /USA[1]/SantaMonica/cole/streets[0]
        /USA[1]/SantaMonica/cole/streets[0][0]
        /USA[1]/SantaMonica/cole/streets[0][0]/name
        /USA[1]/SantaMonica/cole/streets[0][1]
        /USA[1]/SantaMonica/cole/streets[0][1]/name
      */

    //Goal is to encrypt names of all ppl residing in cities named San Francisco in all states of USA
    HasherConfig hasherConfig =
        createInPlaceHasherProcessor(
            ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][*]/name"),
            HashType.SHA2
        );
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);


    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(),
          computeHash(Field.Type.STRING, "jon", HashType.SHA2));
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(),
          computeHash(Field.Type.STRING, "natty", HashType.SHA2));

      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(),
          computeHash(Field.Type.STRING, "adam", HashType.SHA2));
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(),
          computeHash(Field.Type.STRING, "hari", HashType.SHA2));

      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(),
          "madhu");
      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(),
          "girish");

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRegex() {
    String pattern  = "(.*)/name";
    String testString = "/USA[1]/SantaMonica/cole/streets[0][0]/name";
    Pattern compile = Pattern.compile(pattern);
    Matcher matcher = compile.matcher(testString);
    Assert.assertTrue(matcher.matches());
  }

  @Test
  public void testTargetFieldHasherConfig() throws StageException {
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("a", Field.create("a1"));
    map.put("b", Field.create("b1"));
    map.put("c", Field.create("c1"));
    map.put("d", Field.create("d1"));
    map.put("e", Field.create("e1"));
    //Field f will be created later
    map.put("g", Field.create("g1"));
    map.put("h", Field.create("h1"));

    map.put("i", Field.create("i1"));
    map.put("j", Field.create("j1"));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    //Hash Fields a & b together and map to c
    TargetFieldHasherConfig tfieldHasherConfig1 = new TargetFieldHasherConfig();
    tfieldHasherConfig1.sourceFieldsToHash = ImmutableList.of("/a", "/b");
    tfieldHasherConfig1.hashType = HashType.MD5;
    tfieldHasherConfig1.targetField = "/c";
    tfieldHasherConfig1.headerAttribute = "";

    //Hash Fields d & e together and map to f (new field)
    TargetFieldHasherConfig tfieldHasherConfig2 = new TargetFieldHasherConfig();
    tfieldHasherConfig2.sourceFieldsToHash = ImmutableList.of("/d", "/e");
    tfieldHasherConfig2.hashType = HashType.SHA1;
    tfieldHasherConfig2.targetField = "";
    tfieldHasherConfig2.headerAttribute = "/rh1";

    //Hash multiple fields without enabling hashTogether (i.e hash g to g and h to h)
    FieldHasherConfig fieldHasherConfig1 = new FieldHasherConfig();
    fieldHasherConfig1.sourceFieldsToHash = ImmutableList.of("/g", "/h");
    fieldHasherConfig1.hashType = HashType.SHA2;

    //Both fieldToHash and targetField are same.
    TargetFieldHasherConfig tfieldHasherConfig3 = new TargetFieldHasherConfig();
    tfieldHasherConfig3.sourceFieldsToHash = ImmutableList.of("/i");
    tfieldHasherConfig3.hashType = HashType.SHA2;
    tfieldHasherConfig3.targetField = "/i";
    tfieldHasherConfig3.headerAttribute = "";


    HasherConfig hasherConfig = new HasherConfig();
    populateEmptyRecordHasherConfig(hasherConfig);

    hasherConfig.inPlaceFieldHasherConfigs = ImmutableList.of(fieldHasherConfig1);
    hasherConfig.targetFieldHasherConfigs =  ImmutableList.of(tfieldHasherConfig1,
        tfieldHasherConfig2, tfieldHasherConfig3);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    //Check functionality
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Record outputRecord = output.getRecords().get("a").get(0);
      String cVal = outputRecord.get("/c").getValueAsString();
      String rh1Val = outputRecord.getHeader().getAttribute("/rh1");
      String gVal = outputRecord.get("/g").getValueAsString();
      String hVal = outputRecord.get("/h").getValueAsString();
      String iVal = outputRecord.get("/i").getValueAsString();

      Assert.assertTrue(
          cVal.equals(
              computeHashForRecordUsingFields(record,
                  ImmutableList.of("/a", "/b"), HashType.MD5)));
      Assert.assertTrue(
          rh1Val.equals(
              computeHashForRecordUsingFields(record,
                  ImmutableList.of("/d", "/e"), HashType.SHA1)));
      map.put("g", Field.create("g1"));
      map.put("h", Field.create("h1"));

      //g is already hashed so create a new record to check the values are same.
      Record newRecord = RecordCreator.create("s", "s:s1");
      newRecord.set(Field.create(map));

      Assert.assertTrue(
          gVal.equals(
              computeHashForRecordUsingFields(newRecord,
                  ImmutableList.of("/g"), HashType.SHA2)));

      //h is already hashed so create a new record to check the values are same.
      newRecord = RecordCreator.create("s", "s:s1");
      newRecord.set(Field.create(map));
      Assert.assertTrue(
          hVal.equals(computeHashForRecordUsingFields(newRecord,
              ImmutableList.of("/h"), HashType.SHA2)));

      //i is already hashed so create a new record to check the values are same.
      newRecord = RecordCreator.create("s", "s:s1");
      newRecord.set(Field.create(map));
      Assert.assertTrue(
          iVal.equals(computeHashForRecordUsingFields(newRecord,
              ImmutableList.of("/i"), HashType.SHA2)));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRecordHasherConfig() throws StageException {
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("a", Field.create("a1"));
    map.put("b", Field.create("b1"));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));


    HasherConfig hasherConfig = new HasherConfig();
    hasherConfig.recordHasherConfig = new RecordHasherConfig();
    hasherConfig.recordHasherConfig.hashEntireRecord = true;
    hasherConfig.recordHasherConfig.headerAttribute = "/rf1";
    hasherConfig.recordHasherConfig.targetField = "";
    hasherConfig.recordHasherConfig.hashType = HashType.MD5;
    hasherConfig.inPlaceFieldHasherConfigs = Collections.EMPTY_LIST;
    hasherConfig.targetFieldHasherConfigs =  Collections.EMPTY_LIST;

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    //Check functionality
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(
          computeHashForRecordUsingFields(record, record.getFieldPaths(), HashType.MD5),
          output.getRecords().get("a").get(0).getHeader().getAttribute("/rf1")
      );
    } finally {
      runner.runDestroy();
    }


    //There will be a record header field /rf1 already which will also be used for Hashing
    hasherConfig.recordHasherConfig.includeRecordHeaderForHashing = true;
    hasherConfig.recordHasherConfig.headerAttribute = "/rf2";
    record.getHeader().setAttribute("/rf1", "rf1");


    runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      //New record for comparison
      map.clear();
      map.put("a", Field.create("a1"));
      map.put("b", Field.create("b1"));

      record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      record.getHeader().setAttribute("/rf1", "rf1");

      Assert.assertEquals(
          hashForRecordsWithFieldsHeader(
              record,
              record.getFieldPaths(),
              HashType.MD5,
              true
          ),
          output.getRecords().get("a").get(0).getHeader().getAttribute("/rf2")
      );
    } finally {
      runner.runDestroy();
    }

  }

  @Test
  public void testInvalidConfForRecordHasherConfig() throws StageException {
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("a", Field.create("a1"));
    map.put("b", Field.create("b1"));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    HasherConfig hasherConfig = new HasherConfig();
    hasherConfig.recordHasherConfig = new RecordHasherConfig();
    hasherConfig.recordHasherConfig.hashEntireRecord = true;
    hasherConfig.recordHasherConfig.headerAttribute = "";
    hasherConfig.recordHasherConfig.targetField = "";
    hasherConfig.recordHasherConfig.hashType = HashType.MD5;
    hasherConfig.inPlaceFieldHasherConfigs = Collections.EMPTY_LIST;
    hasherConfig.targetFieldHasherConfigs =  Collections.EMPTY_LIST;

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    //Check functionality
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();

    try {
      runner.runInit();
      Assert.fail("Should throw an exception if target field/header attribute is not specified.");
    } catch(StageException e){
      //Expected Exception
    }
  }

  @Test
  public void testInvalidConfForTargetFieldHashing() throws StageException{
    Map<String, Field> map = new LinkedHashMap<>();
    //Check error cases
    map.put("i", Field.create("i1"));
    map.put("j", Field.create("j1"));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    //Hash i & j to a target field with a wild card (Should throw exception as no wild cards are allowed)
    TargetFieldHasherConfig tfieldHasherConfig5 = new TargetFieldHasherConfig();
    tfieldHasherConfig5.sourceFieldsToHash = ImmutableList.of("/i", "/j");
    tfieldHasherConfig5.hashType = HashType.MD5;
    tfieldHasherConfig5.targetField = "/*";
    tfieldHasherConfig5.headerAttribute = "";

    HasherConfig hasherConfig = new HasherConfig();
    populateEmptyRecordHasherConfig(hasherConfig);

    hasherConfig.inPlaceFieldHasherConfigs = Collections.EMPTY_LIST;
    hasherConfig.targetFieldHasherConfigs =  ImmutableList.of(tfieldHasherConfig5);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    try {
      runner.runInit();
      Assert.fail("Should throw an exception if target field is specified as a wild card.");
    } catch(StageException e){
      //Expected Exception
    }


    //Try specifying header attribute as wild card.
    tfieldHasherConfig5.headerAttribute = "/*";
    tfieldHasherConfig5.targetField = "/m";

    runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    try {
      runner.runInit();
      Assert.fail("Should throw an exception if header attribute is specified as a wild card.");
    } catch(StageException e){
      //Expected Exception
    }


    map.clear();
    //Check error cases
    map.put("i", Field.create("i1"));
    map.put("j", Field.create("j1"));

    record = RecordCreator.create("s", "s:2");
    record.set(Field.create(map));

    //Hash i & j to an empty target field and header Attribute
    //(Should throw exception as empty fields are not allowed)
    TargetFieldHasherConfig tfieldHasherConfig6 = new TargetFieldHasherConfig();
    tfieldHasherConfig6.sourceFieldsToHash = ImmutableList.of("/i", "/j");
    tfieldHasherConfig6.hashType = HashType.MD5;
    tfieldHasherConfig6.targetField = "";
    tfieldHasherConfig6.headerAttribute = "";

    hasherConfig = new HasherConfig();
    hasherConfig.recordHasherConfig = new RecordHasherConfig();
    populateEmptyRecordHasherConfig(hasherConfig);

    hasherConfig.inPlaceFieldHasherConfigs = Collections.EMPTY_LIST;
    hasherConfig.targetFieldHasherConfigs =  ImmutableList.of(tfieldHasherConfig6);

    processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("b").build();
    try {
      runner.runInit();
      Assert.fail("Should throw an exception if target field is empty.");
    } catch(StageException e){
      //Expected Exception
    }
  }

}
