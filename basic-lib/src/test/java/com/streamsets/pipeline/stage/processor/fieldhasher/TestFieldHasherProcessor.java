/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.streamsets.pipeline.lib.util.FieldRegexUtil;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FieldHasherProcessor.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestFieldHasherProcessor {

  @Parameterized.Parameters(name = "{0}")
  public static HashType[] data() {
    return HashType.values();
  }

  @Parameterized.Parameter
  public HashType hashType;

  // make switching easy.
  private static boolean OLD_WAY = true;

  private String hashForRecordsWithFieldsAndHeaderAttr(
      Record record,
      Collection<String> fieldsToHash,
      HashType hashType,
      boolean includeRecordHeaderForHashing,
      boolean useSeparator
  ) {
    HashFunction hasher = HashingUtil.getHasher(hashType.getHashType());
    Set<String> validFieldsToHash = new HashSet<>();
    for (String fieldPath : fieldsToHash) {
      Field field = record.get(fieldPath);
      Field.Type type = field.getType();
      if (!(FieldHasherProcessor.UNSUPPORTED_FIELD_TYPES.contains(type) || field.getValue() == null)) {
        validFieldsToHash.add(fieldPath);
      }
    }
    HashingUtil.RecordFunnel recordFunnel =
        HashingUtil.getRecordFunnel(
            validFieldsToHash,
            includeRecordHeaderForHashing,
            useSeparator,
                '\u0000'
        );
    return hasher.hashObject(record, recordFunnel).toString();
  }

  private String computeHashForRecordUsingFields(
      Record record,
      Collection<String> fieldsToHash,
      HashType hashType,
      boolean useSeparator
  ) {
    return hashForRecordsWithFieldsAndHeaderAttr(
        record,
        fieldsToHash,
        hashType,
        false,
        useSeparator
    );
  }

  static String computeHash(Field.Type fieldType, Object value, HashType hashType) {
    HashFunction hasher = HashingUtil.getHasher(hashType.getHashType());
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
      case TIME:
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

    // this was the problem in SDC-6540.
    sink.putByte((byte)0);

    return ((Hasher)sink).hash().toString();
  }

  static void populateEmptyRecordHasherConfig(HasherConfig hasherConfig, HashType hashType) {
    hasherConfig.recordHasherConfig = new RecordHasherConfig();
    hasherConfig.recordHasherConfig.hashEntireRecord = false;
    hasherConfig.recordHasherConfig.headerAttribute = "";
    hasherConfig.recordHasherConfig.targetField = "";
    hasherConfig.recordHasherConfig.hashType = hashType;
    hasherConfig.recordHasherConfig.useSeparator = true;
  }

  static HasherConfig createInPlaceHasherProcessor(
      List<String> sourceFieldsToHash,
      HashType hashType
  ) {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.sourceFieldsToHash = sourceFieldsToHash;
    fieldHasherConfig.hashType = hashType;

    HasherConfig hasherConfig = new HasherConfig();
    hasherConfig.useSeparator = true;
    populateEmptyRecordHasherConfig(hasherConfig, hashType);

    hasherConfig.inPlaceFieldHasherConfigs = ImmutableList.of(fieldHasherConfig);
    hasherConfig.targetFieldHasherConfigs =  Collections.EMPTY_LIST;
    return hasherConfig;
  }

  static HasherConfig createTargetFieldHasherProcessor(
      List<String> sourceFieldsToHash,
      HashType hashType,
      String targetField,
      String headerAttr
  ) {
    HasherConfig hasherConfig = new HasherConfig();
    populateEmptyRecordHasherConfig(hasherConfig, hashType);

    TargetFieldHasherConfig tfieldHasherConfig = new TargetFieldHasherConfig();
    tfieldHasherConfig.sourceFieldsToHash = sourceFieldsToHash;
    tfieldHasherConfig.hashType = hashType;
    tfieldHasherConfig.targetField = targetField;
    tfieldHasherConfig.headerAttribute = headerAttr;
    hasherConfig.inPlaceFieldHasherConfigs = Collections.EMPTY_LIST;
    hasherConfig.targetFieldHasherConfigs =  ImmutableList.of(tfieldHasherConfig);

    hasherConfig.targetFieldHasherConfigs = ImmutableList.of(tfieldHasherConfig);
    hasherConfig.inPlaceFieldHasherConfigs =  Collections.EMPTY_LIST;
    return hasherConfig;
  }

  private HasherConfig createRecordHasherConfig(
      HashType hashType,
      boolean includeRecordHeaderForHashing,
      String targetField,
      String headerAttribute
  ) {
    HasherConfig hasherConfig = new HasherConfig();
    hasherConfig.recordHasherConfig = new RecordHasherConfig();
    hasherConfig.recordHasherConfig.hashEntireRecord = true;
    hasherConfig.recordHasherConfig.includeRecordHeaderForHashing = includeRecordHeaderForHashing;
    hasherConfig.recordHasherConfig.headerAttribute = headerAttribute;
    hasherConfig.recordHasherConfig.targetField = targetField;
    hasherConfig.recordHasherConfig.hashType = hashType;
    hasherConfig.recordHasherConfig.useSeparator = true;   //old way - pre SDC_6540.
    hasherConfig.useSeparator = true;            //old way - pre SDC_6540.


    hasherConfig.inPlaceFieldHasherConfigs = Collections.EMPTY_LIST;
    hasherConfig.targetFieldHasherConfigs =  Collections.EMPTY_LIST;

    return hasherConfig;
  }

  private Set<String> registerCallbackForValidFields() {
    final Set<String> validFieldsFromTheProcessor = new HashSet<String>();
    PowerMockito.replace(
        MemberMatcher.method(
            FieldHasherProcessor.class,
            "validateAndExtractFieldsToHash"
        )
    ).with(
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object returnVal =  method.invoke(proxy, args);
            validFieldsFromTheProcessor.addAll((HashSet<String>) returnVal);
            return returnVal;
          }
        }
    );
    return validFieldsFromTheProcessor;
  }

  private void checkFieldIssueContinue(
      Record record,
      HasherConfig hasherConfig,
      Set<String> expectedValidFields,
      Map<String, Field> expectedVal
  ) throws StageException {
    StageRunner.Output output;
    FieldHasherProcessor processor;
    ProcessorRunner runner;

    final Set<String> validFieldsFromTheProcessor = registerCallbackForValidFields();

    processor = PowerMockito.spy(
        new FieldHasherProcessor(
            hasherConfig,
            OnStagePreConditionFailure.CONTINUE
        )
    );

    runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      output = runner.runProcess(Arrays.asList(record));
      Assert.assertEquals("Valid Fields Size mismatch", expectedValidFields.size(), validFieldsFromTheProcessor.size());
      Assert.assertTrue("Expected Valid Fields Not Present", validFieldsFromTheProcessor.containsAll(expectedValidFields));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record outputRecord = output.getRecords().get("a").get(0);

      Field field = outputRecord.get();
      Assert.assertTrue(field.getValue() instanceof Map);

      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals("Expected fields does not match: ", expectedVal.size(), result.size());

      Set<String> resultFieldPaths = new HashSet<String>();
      for (Map.Entry<String, Field> entry : result.entrySet()) {
        String fieldKey = entry.getKey();
        Field outputField = entry.getValue();
        Field expectedField  = expectedVal.get(fieldKey);
        Assert.assertEquals("Expected Type not present for field:" + fieldKey, expectedField.getType(), outputField.getType());
        Assert.assertEquals("Expected Value not present for field: " + fieldKey, expectedField.getValue(), outputField.getValue());
        resultFieldPaths.add("/" + fieldKey);
      }
    } catch (StageException e) {
      Assert.fail("Should not throw an exception when On Stage Precondition Continue");
    } finally {
      runner.runDestroy();
    }
  }

  private void checkFieldIssueToError(
      Record record,
      HasherConfig hasherConfig,
      Set<String> expectedValidFields) throws StageException {

    StageRunner.Output output;

    final Set<String> validFieldsFromTheProcessor = registerCallbackForValidFields();

    FieldHasherProcessor processor = PowerMockito.spy(
        new FieldHasherProcessor(
            hasherConfig,
            OnStagePreConditionFailure.TO_ERROR
        )
    );

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      output = runner.runProcess(Arrays.asList(record));
      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals("Valid Fields Size mismatch", expectedValidFields.size(), validFieldsFromTheProcessor.size());
      Assert.assertTrue("Expected Valid Fields Not Present", validFieldsFromTheProcessor.containsAll(expectedValidFields));
      Assert.assertEquals(Errors.HASH_01.toString(), runner.getErrorRecords().get(0).getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringFieldWithContinue() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/name"), hashType);
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
      Assert.assertEquals(computeHash(Field.Type.STRING, "streamsets", hashType),
          result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringFieldWithToError() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/name"), hashType);
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.TO_ERROR);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  // SDC-5160
  @Test
  public void testMissingFieldOnContinue() throws StageException {
    HasherConfig hasherConfig = createTargetFieldHasherProcessor(ImmutableList.of("/name"), hashType, "/target", "");
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
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      // Should be no error record
      Assert.assertEquals(0, runner.getErrorRecords().size());

      // With one output record
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record outputRecord = output.getRecords().get("a").get(0);
      Assert.assertFalse("Field /target should not exist", outputRecord.has("/target"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDecimalField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);
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
          computeHash(Field.Type.DECIMAL, new BigDecimal(345.678), hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testIntegerField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
      Assert.assertEquals(computeHash(Field.Type.INTEGER, "-123", hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testShortField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
      Assert.assertEquals(computeHash(Field.Type.SHORT, "123", hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testLongField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
      Assert.assertEquals(computeHash(Field.Type.LONG, "21474836478", hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testByteField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
      Assert.assertEquals(computeHash(Field.Type.BYTE, "125", hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCharField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
      Assert.assertEquals(computeHash(Field.Type.CHAR, "c", hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testBooleanField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
      Assert.assertEquals(computeHash(Field.Type.BOOLEAN, "true", hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDateField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
          computeHash(Field.Type.DATE, new Date(123456789), hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTimeField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.TIME, new Date(123456789)));
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
          computeHash(Field.Type.TIME, new Date(123456789), hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDateTimeField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
          computeHash(Field.Type.DATETIME, new Date(123456789), hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFloatField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
          computeHash(Field.Type.FLOAT, String.valueOf(2.3842e-07f), hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDoubleField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/age"), hashType);

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
      Assert.assertEquals(computeHash(Field.Type.DOUBLE, 12342.3842, hashType),
          result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testByteArrayField() throws StageException {
    HasherConfig hasherConfig = createInPlaceHasherProcessor(ImmutableList.of("/byteArray"), hashType);

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
          computeHash(Field.Type.BYTE_ARRAY, "streamsets".getBytes(), hashType),
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
            hashType
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
      Assert.assertEquals(computeHash(Field.Type.STRING, "a", hashType),
          result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.INTEGER, 21, hashType),
          result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(null, result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "c", hashType),
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
            hashType
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
      Assert.assertEquals(computeHash(Field.Type.STRING, "a", hashType),
          result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(Field.Type.INTEGER, 21, hashType),
          result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(null, result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(computeHash(Field.Type.STRING, "c", hashType),
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
            hashType
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
            hashType
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
  public void testUnsupportedFieldTypes() throws StageException {
    //valid Fields
    final String STRING_FIELD = "stringField";
    final String INT_FIELD = "intField";
    final String BOOLEAN_FIELD = "booleanField";

    //Invalid Fields
    final String NULL_FIELD = "nullField";
    final String LIST_FIELD = "listField";
    final String MAP_FIELD = "mapField";
    final String LIST_MAP_FIELD = "listMapField";

    final String ROOT_PATH = "/";
    final String TARGET_FIELD = "targetField";


    Field stringField = Field.create("string1");
    Field intField = Field.create(1);
    Field booleanField = Field.create(true);
    Field nullField = Field.create(Field.Type.FLOAT, null);

    List<Field> list = new ArrayList<>();
    list.add(Field.create(1));
    list.add(Field.create(2));
    list.add(Field.create(3));
    Field listField = Field.create(list);

    Map<String, Field> map = new HashMap<>();
    map.put("k1", Field.create("v1"));
    map.put("k2", Field.create("v2"));
    map.put("k3", Field.create("v3"));

    Field mapField = Field.create(map);

    LinkedHashMap<String, Field> listMap= new LinkedHashMap<>();
    listMap.put("lk1", Field.create("v1"));
    listMap.put("lk2", Field.create("v2"));
    listMap.put("lk3", Field.create("v3"));
    Field listMapField = Field.createListMap(listMap);

    Map<String, Field> fieldMap = new LinkedHashMap<>();
    fieldMap.put(STRING_FIELD, stringField);
    fieldMap.put(INT_FIELD, intField);
    fieldMap.put(BOOLEAN_FIELD, booleanField);
    fieldMap.put(NULL_FIELD, nullField);
    fieldMap.put(MAP_FIELD, mapField);
    fieldMap.put(LIST_FIELD, listField);
    fieldMap.put(LIST_MAP_FIELD, listMapField);

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(fieldMap));

    final List<String> fieldsToHash = ImmutableList.of(
        ROOT_PATH + STRING_FIELD,
        ROOT_PATH + INT_FIELD,
        ROOT_PATH + BOOLEAN_FIELD,
        ROOT_PATH + NULL_FIELD,
        ROOT_PATH + LIST_FIELD,
        ROOT_PATH + MAP_FIELD,
        ROOT_PATH + LIST_MAP_FIELD
    );

    Set<String> expectedValidFields =  new HashSet<String>();
    expectedValidFields.addAll(FieldRegexUtil.getMatchingFieldPaths(ROOT_PATH + STRING_FIELD, record.getEscapedFieldPaths()));
    expectedValidFields.addAll(FieldRegexUtil.getMatchingFieldPaths(ROOT_PATH + INT_FIELD, record.getEscapedFieldPaths()));
    expectedValidFields.addAll(FieldRegexUtil.getMatchingFieldPaths(ROOT_PATH + BOOLEAN_FIELD, record.getEscapedFieldPaths()));

    //Test HashInPlace
    HasherConfig hasherConfig =
        createInPlaceHasherProcessor(
            fieldsToHash,
            hashType
        );
    hasherConfig.useSeparator = true;

    Map<String, Field> expectedVals = new HashMap<String, Field>();
    expectedVals.put(STRING_FIELD, Field.create(computeHashForRecordUsingFields(record, ImmutableList.of(ROOT_PATH + STRING_FIELD), hashType, OLD_WAY)));
    expectedVals.put(INT_FIELD, Field.create(computeHashForRecordUsingFields(record, ImmutableList.of(ROOT_PATH + INT_FIELD), hashType, OLD_WAY)));
    expectedVals.put(BOOLEAN_FIELD, Field.create(computeHashForRecordUsingFields(record, ImmutableList.of(ROOT_PATH + BOOLEAN_FIELD), hashType, OLD_WAY)));
    expectedVals.put(NULL_FIELD, nullField);
    expectedVals.put(LIST_FIELD, listField);
    expectedVals.put(MAP_FIELD, mapField);
    expectedVals.put(LIST_MAP_FIELD, listMapField);

    checkFieldIssueToError(record, hasherConfig, expectedValidFields);

    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(fieldMap));

    checkFieldIssueContinue(record, hasherConfig, expectedValidFields, expectedVals);

    //Test HashToTarget
    hasherConfig =
        createTargetFieldHasherProcessor(
            fieldsToHash,
            hashType,
            ROOT_PATH + TARGET_FIELD,
            ""
        );
    hasherConfig.useSeparator = true;

    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(fieldMap));
    checkFieldIssueToError(record, hasherConfig, expectedValidFields);

    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(fieldMap));

    expectedVals.clear();
    expectedVals.put(STRING_FIELD, stringField);
    expectedVals.put(INT_FIELD, intField);
    expectedVals.put(BOOLEAN_FIELD, booleanField);
    expectedVals.put(NULL_FIELD, nullField);
    expectedVals.put(LIST_FIELD, listField);
    expectedVals.put(MAP_FIELD, mapField);
    expectedVals.put(LIST_MAP_FIELD, listMapField);
    expectedVals.put(TARGET_FIELD, Field.create(computeHashForRecordUsingFields(record, fieldsToHash, hashType, OLD_WAY)));

    checkFieldIssueContinue(record, hasherConfig, expectedValidFields, expectedVals);

    //Test RecordHasherConfig
    hasherConfig =
        createRecordHasherConfig(
            hashType,
            false,
            ROOT_PATH + TARGET_FIELD,
            ""
        );
    hasherConfig.useSeparator = true;

    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(fieldMap));

    expectedValidFields.clear();
    expectedValidFields.addAll(record.getEscapedFieldPaths());
    expectedValidFields.remove("");
    expectedValidFields.remove(ROOT_PATH + LIST_FIELD);
    expectedValidFields.remove(ROOT_PATH + MAP_FIELD);
    expectedValidFields.remove(ROOT_PATH + LIST_MAP_FIELD);
    expectedValidFields.remove(ROOT_PATH + NULL_FIELD);

    //Check On Field Error, Even specifying error should not throw error for record
    // as we just skip unsupported data types and null fields

    FieldHasherProcessor processor = PowerMockito.spy(
        new FieldHasherProcessor(
            hasherConfig,
            OnStagePreConditionFailure.TO_ERROR
        )
    );

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertEquals(0, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }

    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(fieldMap));

    expectedVals.clear();
    expectedVals.put(STRING_FIELD, stringField);
    expectedVals.put(INT_FIELD, intField);
    expectedVals.put(BOOLEAN_FIELD, booleanField);
    expectedVals.put(NULL_FIELD, nullField);
    expectedVals.put(LIST_FIELD, listField);
    expectedVals.put(MAP_FIELD, mapField);
    expectedVals.put(LIST_MAP_FIELD, listMapField);
    expectedVals.put(TARGET_FIELD, Field.create(computeHashForRecordUsingFields(record, record.getEscapedFieldPaths(), hashType, OLD_WAY)));

    checkFieldIssueContinue(record, hasherConfig, expectedValidFields, expectedVals);
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
            hashType
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
          computeHash(Field.Type.STRING, "jon", hashType));
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(),
          computeHash(Field.Type.STRING, "natty", hashType));

      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(),
          computeHash(Field.Type.STRING, "adam", hashType));
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(),
          computeHash(Field.Type.STRING, "hari", hashType));

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
    tfieldHasherConfig1.hashType = hashType;
    tfieldHasherConfig1.targetField = "/c";
    tfieldHasherConfig1.headerAttribute = "";

    //Hash Fields d & e together and map to f (new field)
    TargetFieldHasherConfig tfieldHasherConfig2 = new TargetFieldHasherConfig();
    tfieldHasherConfig2.sourceFieldsToHash = ImmutableList.of("/d", "/e");
    tfieldHasherConfig2.hashType = hashType;
    tfieldHasherConfig2.targetField = "";
    tfieldHasherConfig2.headerAttribute = "/rh1";

    //Hash multiple fields without enabling hashTogether (i.e hash g to g and h to h)
    FieldHasherConfig fieldHasherConfig1 = new FieldHasherConfig();
    fieldHasherConfig1.sourceFieldsToHash = ImmutableList.of("/g", "/h");
    fieldHasherConfig1.hashType = hashType;

    //Both fieldToHash and targetField are same.
    TargetFieldHasherConfig tfieldHasherConfig3 = new TargetFieldHasherConfig();
    tfieldHasherConfig3.sourceFieldsToHash = ImmutableList.of("/i");
    tfieldHasherConfig3.hashType = hashType;
    tfieldHasherConfig3.targetField = "/i";
    tfieldHasherConfig3.headerAttribute = "";


    HasherConfig hasherConfig = new HasherConfig();
    populateEmptyRecordHasherConfig(hasherConfig, hashType);

    hasherConfig.inPlaceFieldHasherConfigs = ImmutableList.of(fieldHasherConfig1);
    hasherConfig.targetFieldHasherConfigs =  ImmutableList.of(tfieldHasherConfig1,
        tfieldHasherConfig2, tfieldHasherConfig3);
    hasherConfig.useSeparator = true;   // old way - prior to SDC-6540.

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

      Assert.assertEquals(
          cVal,
          computeHashForRecordUsingFields(record, ImmutableList.of("/a", "/b"), hashType, OLD_WAY)
      );
      Assert.assertEquals(
          rh1Val,
          computeHashForRecordUsingFields(record, ImmutableList.of("/d", "/e"), hashType, OLD_WAY)
      );
      map.put("g", Field.create("g1"));
      map.put("h", Field.create("h1"));

      //g is already hashed so create a new record to check the values are same.
      Record newRecord = RecordCreator.create("s", "s:s1");
      newRecord.set(Field.create(map));

      Assert.assertEquals(
          gVal,
          computeHashForRecordUsingFields(newRecord, ImmutableList.of("/g"), hashType, OLD_WAY)
      );

      //h is already hashed so create a new record to check the values are same.
      newRecord = RecordCreator.create("s", "s:s1");
      newRecord.set(Field.create(map));
      Assert.assertEquals(
          hVal,
          computeHashForRecordUsingFields(newRecord, ImmutableList.of("/h"), hashType, OLD_WAY)
      );

      //i is already hashed so create a new record to check the values are same.
      newRecord = RecordCreator.create("s", "s:s1");
      newRecord.set(Field.create(map));
      Assert.assertEquals(
          iVal,
          computeHashForRecordUsingFields(newRecord, ImmutableList.of("/i"), hashType, OLD_WAY)
      );

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

    HasherConfig hasherConfig = createRecordHasherConfig(hashType, false, "", "/rf1");

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    //Check functionality
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(
          computeHashForRecordUsingFields(record, record.getEscapedFieldPaths(), hashType, OLD_WAY),
          output.getRecords().get("a").get(0).getHeader().getAttribute("/rf1")
      );
    } finally {
      runner.runDestroy();
    }


    //There will be a record header field /rf1 already which will also be used for Hashing
    record.getHeader().setAttribute("/rf1", "rf1");
    hasherConfig = createRecordHasherConfig(hashType, true, "", "/rf2");

    processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

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
          hashForRecordsWithFieldsAndHeaderAttr(
              record,
              record.getEscapedFieldPaths(),
              hashType,
              true,
              OLD_WAY
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

    HasherConfig hasherConfig = createRecordHasherConfig(hashType, false, "", "");

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

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
    HasherConfig hasherConfig = createTargetFieldHasherProcessor(
        ImmutableList.of("/i", "/j"),
        hashType,
        "/*",
        ""
    );

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
    hasherConfig = createTargetFieldHasherProcessor(
        ImmutableList.of("/i", "/j"),
        hashType,
        "/*",
        "/m"
    );

    processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);
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
    hasherConfig = createTargetFieldHasherProcessor(
        ImmutableList.of("/i", "/j"),
        hashType,
        "",
        ""
    );
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
