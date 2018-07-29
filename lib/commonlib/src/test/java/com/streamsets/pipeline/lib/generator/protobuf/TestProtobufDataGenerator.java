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
package com.streamsets.pipeline.lib.generator.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.util.ProtobufConstants;
import com.streamsets.pipeline.lib.util.ProtobufTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

public class TestProtobufDataGenerator {

  @Test
  public void writeWithOneOfAndMap() throws Exception {
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    byte[] expected = FileUtils.readFileToByteArray(new File(Resources.getResource("TestProtobuf3.ser").getPath()));
    DataGenerator dataGenerator = getDataGenerator(bOut, "TestRecordProtobuf3.desc", "TestRecord", true);

    Record record = getContext().createRecord("");
    Map<String, Field> rootField = new HashMap<>();
    rootField.put("first_name", Field.create("Adam"));
    rootField.put("full_name", Field.create(Field.Type.STRING, null));
    rootField.put("samples", Field.create(ImmutableList.of(Field.create(1), Field.create(2))));
    Map<String, Field> entries = ImmutableMap.of(
        "hello", Field.create("world"),
        "bye", Field.create("earth")
    );
    rootField.put("test_map", Field.create(entries));
    record.set(Field.create(rootField));

    dataGenerator.write(record);
    dataGenerator.flush();
    dataGenerator.close();

    assertArrayEquals(expected, bOut.toByteArray());
  }

  @Test
  public void testProtobufDataGenerator() throws Exception {
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();

    // create mock sdc records that mimic records parsed from protobuf represented by Employee.desc file.
    List<Record> records = ProtobufTestUtil.getProtobufRecords();

    // write these records using the protobuf generator
    DataGenerator gen = getDataGenerator(bOut, "Employee.desc", "util.Employee", true);
    for(Record r : records) {
      gen.write(r);
    }
    gen.flush();
    gen.close();

    // parse the output stream using protobuf APIs to get dynamic messages
    ProtobufTestUtil.checkProtobufDataFields(bOut.toByteArray());
    ProtobufTestUtil.checkProtobufDataExtensions(bOut.toByteArray());
    ProtobufTestUtil.checkProtobufDataUnknownFields(bOut.toByteArray());
  }

  @Test
  public void testProtobufDataGeneratorNonDelimited() throws Exception {

    ByteArrayOutputStream bOut = new ByteArrayOutputStream();

    // create mock sdc records that mimic records parsed from protobuf represented by Employee.desc file.
    List<Record> records = ProtobufTestUtil.getProtobufRecords();

    // write these records using the protobuf generator
    DataGenerator gen = getDataGenerator(bOut, "Employee.desc", "util.Employee", false);
    gen.write(records.get(0));

    ProtobufTestUtil.checkSingleNonDelimitedMessage(bOut.toByteArray());

  }

  @Test
  public void testProtobufNonRepeatedField() throws Exception {
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    byte[] expected = FileUtils.readFileToByteArray(new File(Resources.getResource("TestProtobuf3_no_repeated.ser").getPath()));
    DataGenerator dataGenerator = getDataGenerator(bOut, "TestRecordProtobuf3.desc", "TestRecord", true);

    Record record = getContext().createRecord("");
    // "samples" field has repeated keyword, so we won't include that in this record
    Map<String, Field> rootField = new HashMap<>();
    rootField.put("first_name", Field.create("Adam"));
    rootField.put("full_name", Field.create(Field.Type.STRING, null));
    Map<String, Field> entries = ImmutableMap.of(
        "hello", Field.create("world"),
        "bye", Field.create("earth")
    );
    rootField.put("test_map", Field.create(entries));
    record.set(Field.create(rootField));

    dataGenerator.write(record);
    dataGenerator.flush();
    dataGenerator.close();

    assertArrayEquals(expected, bOut.toByteArray());
  }


  public DataGenerator getDataGenerator(OutputStream os, String protoFile, String messageType, boolean isDelimited)
      throws IOException, DataParserException {
    return getDataGeneratorFactory(protoFile, messageType, isDelimited).getGenerator(os);
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  public DataGeneratorFactory getDataGeneratorFactory(String protoFile, String messageType, boolean isDelimited) {
    DataGeneratorFactoryBuilder dataGenFactoryBuilder = new DataGeneratorFactoryBuilder(getContext(),
      DataGeneratorFormat.PROTOBUF);
    DataGeneratorFactory factory = dataGenFactoryBuilder
      .setConfig(ProtobufConstants.PROTO_DESCRIPTOR_FILE_KEY, Resources.getResource(protoFile).getPath())
      .setConfig(ProtobufConstants.MESSAGE_TYPE_KEY, messageType)
      .setConfig(ProtobufConstants.DELIMITED_KEY, isDelimited)
      .build();
    return factory;
  }

}
