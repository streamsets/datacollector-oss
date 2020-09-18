/*
 * Copyright 2018 StreamSets Inc.
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
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TestFieldHasherProcessorSpecific.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestFieldHasherProcessorSpecific {

  final String DATA1 = "one";
  final String DATA2 = "two";
  final String DATA3 = "three";

  @Test
  public void testMultipleFieldsDifferentHashTypes() throws StageException {
    FieldHasherConfig sha1HasherConfig = new FieldHasherConfig();
    sha1HasherConfig.sourceFieldsToHash = ImmutableList.of("/age", "/name");
    sha1HasherConfig.hashType = HashType.SHA1;

    FieldHasherConfig sha2HasherConfig = new FieldHasherConfig();
    sha2HasherConfig.sourceFieldsToHash = ImmutableList.of("/sex");
    sha2HasherConfig.hashType = HashType.SHA256;

    FieldHasherConfig sha512HasherConfig = new FieldHasherConfig();
    sha512HasherConfig.sourceFieldsToHash = ImmutableList.of("/nickName");
    sha512HasherConfig.hashType = HashType.SHA512;

    FieldHasherConfig md5HasherConfig = new FieldHasherConfig();
    md5HasherConfig.sourceFieldsToHash = ImmutableList.of("/streetAddress");
    md5HasherConfig.hashType = HashType.MD5;

    FieldHasherConfig murmur3HasherConfig = new FieldHasherConfig();
    murmur3HasherConfig.sourceFieldsToHash = ImmutableList.of("/birthYear");
    murmur3HasherConfig.hashType = HashType.MURMUR3_128;

    HasherConfig hasherConfig = new HasherConfig();
    TestFieldHasherProcessor.populateEmptyRecordHasherConfig(hasherConfig, HashType.MD5);

    hasherConfig.inPlaceFieldHasherConfigs = ImmutableList.of(
        sha1HasherConfig, sha2HasherConfig, sha512HasherConfig, md5HasherConfig, murmur3HasherConfig);
    hasherConfig.targetFieldHasherConfigs =  Collections.EMPTY_LIST;
    hasherConfig.useSeparator = true;   //old way.  pre SDC-6540.

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("sex", Field.create(Field.Type.STRING, "male"));
      map.put("nickName", Field.create(Field.Type.STRING, "b"));
      map.put("streetAddress", Field.create("sansome street"));
      map.put("birthYear", Field.create(1988));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(6, result.size());
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(TestFieldHasherProcessor.computeHash(Field.Type.STRING, "a", HashType.SHA1),
          result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(TestFieldHasherProcessor.computeHash(Field.Type.INTEGER, 21, HashType.SHA1),
          result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(TestFieldHasherProcessor.computeHash(Field.Type.STRING, "male", HashType.SHA256),
          result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("nickName"));
      Assert.assertEquals(TestFieldHasherProcessor.computeHash(Field.Type.STRING, "b", HashType.SHA512),
          result.get("nickName").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(TestFieldHasherProcessor.computeHash(Field.Type.STRING, "sansome street", HashType.MD5),
          result.get("streetAddress").getValue());
      Assert.assertTrue(result.containsKey("birthYear"));
      Assert.assertEquals(TestFieldHasherProcessor.computeHash(Field.Type.INTEGER, 1988, HashType.MURMUR3_128),
          result.get("birthYear").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultiStringMD5NoSeparator() throws StageException {
    // new way - after SDC-6540.
    HasherConfig hasherConfig = TestFieldHasherProcessor.createTargetFieldHasherProcessor(
        ImmutableList.of("/x", "/y", "/z"),
        HashType.MD5,
        "/hash",
        ""
    );
    hasherConfig.useSeparator = false;
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Record record = createRec(DATA1, DATA2, DATA3);

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);

      Map<String, Field> result = field.getValueAsMap();
      verifyResult(result);

      Assert.assertEquals(DigestUtils.md5Hex(DATA1+DATA2+DATA3), result.get("hash").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringForMD5WithSeparator() throws StageException {
    // this is the MD5 calculation prior to applying SDC-6540
    final String DATA = "help";
    final String OLD_WAY_MD5 = "e5840f5df62e7dd7644fc085774cad00";
    HasherConfig hasherConfig = TestFieldHasherProcessor.createInPlaceHasherProcessor(
        ImmutableList.of("/x"), HashType.MD5);
    hasherConfig.useSeparator = true;
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("x", Field.create(Field.Type.STRING, DATA));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("x"));

      Assert.assertEquals(OLD_WAY_MD5, result.get("x").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultiStringMD5WithNullSeparator() throws StageException {
    final String HASH = "20203d8ffaeb6033607ae5f4b3a9d8cd";
    HasherConfig hasherConfig = TestFieldHasherProcessor.createTargetFieldHasherProcessor(
        ImmutableList.of("/x", "/y", "/z"),
        HashType.MD5,
        "/hash",
        ""
    );
    hasherConfig.useSeparator = true;
    // with default value for hasherConfig.separatorCharacter -- should be null.
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {

      Record record = createRec(DATA1, DATA2, DATA3);

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);

      Map<String, Field> result = field.getValueAsMap();
      verifyResult(result);

      Assert.assertEquals(HASH, result.get("hash").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultiStringMD5WithPipeSeparator() throws StageException {
    final String HASH = "b078798a7e3947c2e6688bcef15999a9";
    HasherConfig hasherConfig = TestFieldHasherProcessor.createTargetFieldHasherProcessor(
        ImmutableList.of("/x", "/y", "/z"),
        HashType.MD5,
        "/hash",
        ""
    );
    hasherConfig.useSeparator = true;
    hasherConfig.separatorCharacter = '|';
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {

      Record record = createRec(DATA1, DATA2, DATA3);

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);

      Map<String, Field> result = field.getValueAsMap();
      verifyResult(result);

      Assert.assertEquals(HASH, result.get("hash").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringForMD5NoSeparator() throws StageException {
    final String DATA = "help";
    HasherConfig hasherConfig = TestFieldHasherProcessor.createInPlaceHasherProcessor(
        ImmutableList.of("/x"), HashType.MD5);
    hasherConfig.useSeparator = false;

    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("x", Field.create(Field.Type.STRING, DATA));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("x"));

      Assert.assertEquals(DigestUtils.md5Hex(DATA), result.get("x").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultiStringMD5WithSnowmanSeparator() throws StageException {
    final String HASH = "c2cfa6fe1276b6954a1162cb8d5ac17c";
    HasherConfig hasherConfig = TestFieldHasherProcessor.createTargetFieldHasherProcessor(
        ImmutableList.of("/x", "/y", "/z"),
        HashType.MD5,
        "/hash",
        ""
    );
    hasherConfig.useSeparator = true;
    hasherConfig.separatorCharacter = '\u26c4';
    FieldHasherProcessor processor = new FieldHasherProcessor(hasherConfig, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Record record = createRec(DATA1, DATA2, DATA3);

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);

      Map<String, Field> result = field.getValueAsMap();
      verifyResult(result);

      Assert.assertEquals(HASH, result.get("hash").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  private Record createRec(String data1, String data2, String data3) {
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("x", Field.create(Field.Type.STRING, data1));
    map.put("y", Field.create(Field.Type.STRING, data2));
    map.put("z", Field.create(Field.Type.STRING, data3));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    return record;
  }

  private void verifyResult(Map<String, Field> result) {
    Assert.assertTrue(result.size() == 4);   //field count
    Assert.assertTrue(result.containsKey("x"));
    Assert.assertTrue(result.containsKey("y"));
    Assert.assertTrue(result.containsKey("z"));
    Assert.assertTrue(result.containsKey("hash"));
  }
}
