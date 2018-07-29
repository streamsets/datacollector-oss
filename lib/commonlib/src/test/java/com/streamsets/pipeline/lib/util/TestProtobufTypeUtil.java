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
package com.streamsets.pipeline.lib.util;

import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestProtobufTypeUtil {

  private final Map<String, Set<Descriptors.FileDescriptor>> fileDescriptorDependentsMap = new HashMap<>();
  private final Map<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
  private final Map<String, Object> defaultValueMap = new HashMap<>();
  private final Map<String, Set<Descriptors.FieldDescriptor>> typeToExtensionMap = new HashMap<>();
  private DescriptorProtos.FileDescriptorSet set;
  private Descriptors.Descriptor md;
  private ExtensionRegistry extensionRegistry;


  @Before
  public void setUp() throws Exception {
    FileInputStream fin = new FileInputStream(Resources.getResource("Employee.desc").getPath());
    set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
    ProtobufTypeUtil.getAllFileDescriptors(set, fileDescriptorDependentsMap, fileDescriptorMap);
    ProtobufTypeUtil.populateDefaultsAndExtensions(fileDescriptorMap, typeToExtensionMap, defaultValueMap);
    md = ProtobufTypeUtil.getDescriptor(set, fileDescriptorMap, "Employee.desc", "util.Employee");
    extensionRegistry = ProtobufTestUtil.createExtensionRegistry(typeToExtensionMap);
  }

  @Test
  public void testDescriptorDependentsMap() throws Exception {

    Assert.assertTrue(!fileDescriptorDependentsMap.isEmpty());
    Assert.assertEquals(0, fileDescriptorDependentsMap.get("Person.proto").size());

    Assert.assertEquals(1, fileDescriptorDependentsMap.get("Engineer.proto").size());
    Set<Descriptors.FileDescriptor> fileDescriptors = fileDescriptorDependentsMap.get("Engineer.proto");
    Iterator<Descriptors.FileDescriptor> iterator = fileDescriptors.iterator();
    Assert.assertEquals("Person.proto", iterator.next().getName());

    Assert.assertEquals(1, fileDescriptorDependentsMap.get("Executive.proto").size());
    fileDescriptors = fileDescriptorDependentsMap.get("Executive.proto");
    iterator = fileDescriptors.iterator();
    Assert.assertEquals("Person.proto", iterator.next().getName());

    Assert.assertEquals(3, fileDescriptorDependentsMap.get("Employee.proto").size());
    fileDescriptors = fileDescriptorDependentsMap.get("Employee.proto");
    iterator = fileDescriptors.iterator();
    Assert.assertEquals("Engineer.proto", iterator.next().getName());
    Assert.assertEquals("Executive.proto", iterator.next().getName());
    Assert.assertEquals("Person.proto", iterator.next().getName());


    Assert.assertEquals(4, fileDescriptorDependentsMap.get("Extensions.proto").size());
    fileDescriptors = fileDescriptorDependentsMap.get("Extensions.proto");
    iterator = fileDescriptors.iterator();
    Assert.assertEquals("Person.proto", iterator.next().getName());
    Assert.assertEquals("Engineer.proto", iterator.next().getName());
    Assert.assertEquals("Executive.proto", iterator.next().getName());
    Assert.assertEquals("Employee.proto", iterator.next().getName());

  }

  @Test
  public void testFileDescriptorMap() throws Exception {

    Assert.assertEquals(5, fileDescriptorMap.size());
    Assert.assertTrue(fileDescriptorMap.containsKey("Person.proto"));
    Assert.assertTrue(fileDescriptorMap.containsKey("Engineer.proto"));
    Assert.assertTrue(fileDescriptorMap.containsKey("Executive.proto"));
    Assert.assertTrue(fileDescriptorMap.containsKey("Employee.proto"));
    Assert.assertTrue(fileDescriptorMap.containsKey("Extensions.proto"));
  }

  @Test
  public void testTypeToExtensions() throws Exception {

    Assert.assertEquals(4, typeToExtensionMap.size());

    Set<Descriptors.FieldDescriptor> fieldDescriptors = typeToExtensionMap.get("util.Person");
    Assert.assertEquals(1, fieldDescriptors.size());
    Iterator<Descriptors.FieldDescriptor> iterator = fieldDescriptors.iterator();
    Assert.assertEquals("util.PersonExtension.NestedPersonExtension.residenceAddress", iterator.next().getFullName());

    fieldDescriptors = typeToExtensionMap.get("util.Engineer");
    Assert.assertEquals(1, fieldDescriptors.size());
    iterator = fieldDescriptors.iterator();
    Assert.assertEquals("util.EngineerExtension.factoryAddress", iterator.next().getFullName());

    fieldDescriptors = typeToExtensionMap.get("util.Executive");
    Assert.assertEquals(1, fieldDescriptors.size());
    iterator = fieldDescriptors.iterator();
    Assert.assertEquals("util.ExecutiveExtension.officeAddress", iterator.next().getFullName());

    fieldDescriptors = typeToExtensionMap.get("util.Employee");
    Assert.assertEquals(7, fieldDescriptors.size());
    iterator = fieldDescriptors.iterator();
    Assert.assertEquals("util.stringField", iterator.next().getFullName());
    Assert.assertEquals("util.doubleField", iterator.next().getFullName());
    Assert.assertEquals("util.floatField", iterator.next().getFullName());
    Assert.assertEquals("util.boolField", iterator.next().getFullName());
    Assert.assertEquals("util.intField", iterator.next().getFullName());
    Assert.assertEquals("util.longField", iterator.next().getFullName());
    Assert.assertEquals("util.bytesField", iterator.next().getFullName());
  }

  @Test
  public void testDefaultValues() throws Exception {

    Assert.assertEquals(9, defaultValueMap.size());
    Assert.assertEquals(
        "HOME",
        ((Descriptors.EnumValueDescriptor)defaultValueMap.get("util.Person.PhoneNumber.type")).getName()
    );
    Assert.assertEquals("engineering", defaultValueMap.get("util.Engineer.depName"));

    Assert.assertEquals("NY", defaultValueMap.get("util.Employee.stringField"));
    Assert.assertEquals(43243, defaultValueMap.get("util.Employee.intField"));
    Assert.assertEquals(3534.234, defaultValueMap.get("util.Employee.doubleField"));
    Assert.assertEquals(true, defaultValueMap.get("util.Employee.boolField"));
    Assert.assertEquals(343.34f, defaultValueMap.get("util.Employee.floatField"));
    Assert.assertEquals(2343254354L, defaultValueMap.get("util.Employee.longField"));
    Assert.assertTrue(
        Arrays.equals(
            "NewYork".getBytes(),
            ((ByteString) defaultValueMap.get("util.Employee.bytesField")).toByteArray()
        )
    );
  }

  @Test
  public void testProtoToSdcMessageFields() throws Exception {

    List<DynamicMessage> messages = ProtobufTestUtil.getMessages(
      md,
      extensionRegistry,
      ProtobufTestUtil.getProtoBufData()
    );

    for (int i = 0; i < messages.size(); i++) {
      DynamicMessage m = messages.get(i);
      Record record = RecordCreator.create();
      Field field = ProtobufTypeUtil.protobufToSdcField(record, "", md, typeToExtensionMap, m);
      Assert.assertNotNull(field);
      ProtobufTestUtil.checkProtobufRecords(field, i);
    }
  }

  @Test
  public void testProtoToSdcExtensionFields() throws Exception {

    List<DynamicMessage> messages = ProtobufTestUtil.getMessages(
      md,
      extensionRegistry,
      ProtobufTestUtil.getProtoBufData()
    );

    for (int i = 0; i < messages.size(); i++) {
      DynamicMessage m = messages.get(i);
      Record record = RecordCreator.create();
      Field field = ProtobufTypeUtil.protobufToSdcField(record, "", md, typeToExtensionMap, m);
      Assert.assertNotNull(field);
      ProtobufTestUtil.checkProtobufRecordsForExtensions(field, i);
    }
  }

  @Test
  public void testProtoToSdcUnknownFields() throws Exception {

    List<DynamicMessage> messages = ProtobufTestUtil.getMessages(
      md,
      extensionRegistry,
      ProtobufTestUtil.getProtoBufData()
    );

    for (int i = 0; i < messages.size(); i++) {
      DynamicMessage m = messages.get(i);
      Record record = RecordCreator.create();
      ProtobufTypeUtil.protobufToSdcField(record, "", md, typeToExtensionMap, m);
      ProtobufTestUtil.checkRecordForUnknownFields(record, i);
    }
  }

  @Test
  public void testSdcToProtobufFields() throws Exception {

    List<Record> protobufRecords = ProtobufTestUtil.getProtobufRecords();
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(bOut);
    for (int i = 0; i < protobufRecords.size(); i++) {
      DynamicMessage dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
          protobufRecords.get(i),
          md,
          typeToExtensionMap,
          defaultValueMap
      );

      dynamicMessage.writeDelimitedTo(bufferedOutputStream);
    }
    bufferedOutputStream.flush();
    bufferedOutputStream.close();
    ProtobufTestUtil.checkProtobufDataFields(bOut.toByteArray());
  }

  @Test
  public void testSdcToProtobufExtensions() throws Exception {
    List<Record> protobufRecords = ProtobufTestUtil.getProtobufRecords();
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(bOut);
    for (int i = 0; i < protobufRecords.size(); i++) {
      DynamicMessage dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
        protobufRecords.get(i),
        md,
        typeToExtensionMap,
        defaultValueMap
      );

      dynamicMessage.writeDelimitedTo(bufferedOutputStream);
    }
    bufferedOutputStream.flush();
    bufferedOutputStream.close();
    ProtobufTestUtil.checkProtobufDataExtensions(bOut.toByteArray());
  }

  @Test
  public void testSdcToProtobufUnknownFields() throws Exception {

    List<Record> protobufRecords = ProtobufTestUtil.getProtobufRecords();
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(bOut);
    for (int i = 0; i < protobufRecords.size(); i++) {
      DynamicMessage dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
        protobufRecords.get(i),
        md,
        typeToExtensionMap,
        defaultValueMap
      );

      dynamicMessage.writeDelimitedTo(bufferedOutputStream);
    }
    bufferedOutputStream.flush();
    bufferedOutputStream.close();
    ProtobufTestUtil.checkProtobufDataUnknownFields(bOut.toByteArray());
  }

  // Tests for repeated fields
  @Test
  public void testNullRepeated() throws DataGeneratorException {
    Record r = RecordCreator.create();
    Map<String, Field> repeated = new HashMap<>();
    repeated.put("samples", Field.create(Field.Type.LIST, null));
    r.set(Field.create(repeated));
    Descriptors.Descriptor descriptor = RepeatedProto.getDescriptor().findMessageTypeByName("Repeated");
    // repeated field samples is null and ignored
    DynamicMessage dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
      r,
      descriptor,
      typeToExtensionMap,
      defaultValueMap
    );
    // null repeated fields are treated as empty arrays
    Object samples = dynamicMessage.getField(descriptor.findFieldByName("samples"));
    Assert.assertNotNull(samples);
    Assert.assertTrue(samples instanceof List);
    Assert.assertEquals(0, ((List) samples).size());
  }

  @Test
  public void testEmptyRepeated() throws DataGeneratorException {
    Record r = RecordCreator.create();
    Map<String, Field> repeated = new HashMap<>();
    repeated.put("samples", Field.create(Field.Type.LIST, new ArrayList<>()));
    r.set(Field.create(repeated));
    Descriptors.Descriptor descriptor = RepeatedProto.getDescriptor().findMessageTypeByName("Repeated");
    // repeated field samples is null and ignored
    DynamicMessage dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
      r,
      descriptor,
      typeToExtensionMap,
      defaultValueMap
    );
    // null repeated fields are treated as empty arrays
    Object samples = dynamicMessage.getField(descriptor.findFieldByName("samples"));
    Assert.assertNotNull(samples);
    Assert.assertTrue(samples instanceof List);
    Assert.assertEquals(0, ((List)samples).size());
  }

  @Test
  public void testNonEmptyRepeated() throws DataGeneratorException {
    Record r = RecordCreator.create();
    Map<String, Field> repeated = new HashMap<>();
    repeated.put(
        "samples",
        Field.create(
            Field.Type.LIST,
            Arrays.asList(
                Field.create(1),
                Field.create(2),
                Field.create(3),
                Field.create(4),
                Field.create(5)
            )
        )
    );
    r.set(Field.create(repeated));
    Descriptors.Descriptor descriptor = RepeatedProto.getDescriptor().findMessageTypeByName("Repeated");
    // repeated field samples is null and ignored
    DynamicMessage dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
      r,
      descriptor,
      typeToExtensionMap,
      defaultValueMap
    );
    // null repeated fields are treated as empty arrays
    Object samples = dynamicMessage.getField(descriptor.findFieldByName("samples"));
    Assert.assertNotNull(samples);
    Assert.assertTrue(samples instanceof List);
    Assert.assertEquals(5, ((List)samples).size());
  }

  // Tests for Oneof s
  @Test
  public void testOneofProtoToSdc() throws DataParserException, IOException, DataGeneratorException {

    Descriptors.Descriptor descriptor = OneofProto.getDescriptor().findMessageTypeByName("Oneof");
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    OneofProto.Oneof.Builder builder = OneofProto.Oneof.newBuilder();

    OneofProto.Oneof build = builder.setOneofInt(5).build();
    build.writeDelimitedTo(bOut);
    bOut.close();

    DynamicMessage.Builder dynBldr = DynamicMessage.newBuilder(descriptor);
    dynBldr.mergeDelimitedFrom(new ByteArrayInputStream(bOut.toByteArray()), null);
    Record record = RecordCreator.create();
    Field field = ProtobufTypeUtil.protobufToSdcField(record, "", descriptor, typeToExtensionMap, dynBldr.build());
    Assert.assertNotNull(field);
    Assert.assertEquals("", field.getValueAsMap().get("oneofString").getValue());
    Assert.assertEquals(Field.Type.INTEGER, field.getValueAsListMap().get("oneofInt").getType());
    Assert.assertEquals(5, field.getValueAsMap().get("oneofInt").getValueAsInteger());

    bOut.reset();
    builder.clear();
    build = builder.setOneofString("Hello").build();
    build.writeDelimitedTo(bOut);
    bOut.close();

    dynBldr = DynamicMessage.newBuilder(descriptor);
    dynBldr.mergeDelimitedFrom(new ByteArrayInputStream(bOut.toByteArray()), null);
    record = RecordCreator.create();
    field = ProtobufTypeUtil.protobufToSdcField(record, "", descriptor, typeToExtensionMap, dynBldr.build());
    Assert.assertNotNull(field);
    Assert.assertEquals(0, field.getValueAsMap().get("oneofInt").getValue());
    Assert.assertEquals(Field.Type.STRING, field.getValueAsListMap().get("oneofString").getType());
    Assert.assertEquals("Hello", field.getValueAsMap().get("oneofString").getValueAsString());

  }

  @Test
  public void testOneofSdcToProtobuf() throws DataGeneratorException {

    Record r1 = RecordCreator.create();
    Map<String, Field> oneofInt = new HashMap<>();
    oneofInt.put("oneofInt", Field.create(5));
    r1.set(Field.create(oneofInt));

    Record r2 = RecordCreator.create();
    Map<String, Field> oneofString = new HashMap<>();
    oneofString.put("oneofString", Field.create("Hello"));
    r2.set(Field.create(oneofString));

    Record r3 = RecordCreator.create();
    Map<String, Field> oneof = new HashMap<>();
    oneof.put("oneofInt", Field.create(5));
    oneof.put("oneofString", Field.create("Hello"));
    r3.set(Field.create(oneof));

    Descriptors.Descriptor descriptor = OneofProto.getDescriptor().findMessageTypeByName("Oneof");

    // in r1 oneofInt field is set
    DynamicMessage dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
        r1,
        descriptor,
        typeToExtensionMap,
        defaultValueMap
    );
    Object oneof_name = dynamicMessage.getField(descriptor.findFieldByName("oneofString"));
    Assert.assertNotNull(oneof_name);
    oneof_name = dynamicMessage.getField(descriptor.findFieldByName("oneofInt"));
    Assert.assertNotNull(oneof_name);
    Assert.assertTrue(oneof_name instanceof Integer);
    Assert.assertEquals(5, (int) oneof_name);

    // in r2 oneofString field is set
    dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
        r2,
        descriptor,
        typeToExtensionMap,
        defaultValueMap
    );
    oneof_name = dynamicMessage.getField(descriptor.findFieldByName("oneofInt"));
    Assert.assertNotNull(oneof_name);
    oneof_name = dynamicMessage.getField(descriptor.findFieldByName("oneofString"));
    Assert.assertNotNull(oneof_name);
    Assert.assertTrue(oneof_name instanceof String);
    Assert.assertEquals("Hello", oneof_name);

    // Oneof.proto defines one fields in the this order:
    // oneof oneof_name {
    //    int32 oneofInt = 10;
    //    string oneofString = 2;
    // }
    // Therefore when both fields are set the String field is expected to take over because
    // ProtobufTypeUtil.sdcFieldToProtobufMsg sets fields in the order of declaration in the proto file
    // Note that field number does not matter, order of declaration matters.
    dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
      r3,
      descriptor,
      typeToExtensionMap,
      defaultValueMap
    );
    oneof_name = dynamicMessage.getField(descriptor.findFieldByName("oneofInt"));
    Assert.assertNotNull(oneof_name);
    oneof_name = dynamicMessage.getField(descriptor.findFieldByName("oneofString"));
    Assert.assertNotNull(oneof_name);
    Assert.assertTrue(oneof_name instanceof String);
    Assert.assertEquals("Hello", oneof_name);
  }

  @Test
  public void testDefaultValueByteString() throws Exception {
    Record writtenRecord = RecordCreator.create();
    Map<String, Field> oneofInt = new HashMap<>();
    oneofInt.put("id", Field.create(5));
    writtenRecord.set(Field.create(oneofInt));

    Map<String, Object> defaultValueMap = new HashMap<>();
    Map<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
    Map<String, Set<Descriptors.FieldDescriptor>> typeToExtensionMap = new HashMap<>();

    FileInputStream fin = new FileInputStream(Resources.getResource("SampleV1.desc").getPath());

    DescriptorProtos.FileDescriptorSet fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
    ProtobufTypeUtil.getAllFileDescriptors(fileDescriptorSet, new HashMap<>(), fileDescriptorMap);
    ProtobufTypeUtil.populateDefaultsAndExtensions(fileDescriptorMap, typeToExtensionMap, defaultValueMap);

    Descriptors.Descriptor descriptor =
        ProtobufTypeUtil.getDescriptor(fileDescriptorSet, fileDescriptorMap, "SampleV1.desc", "util.Sample");

    ByteArrayOutputStream bOut = new ByteArrayOutputStream();

    DynamicMessage dynamicMessage = ProtobufTypeUtil.sdcFieldToProtobufMsg(
        writtenRecord,
        descriptor,
        typeToExtensionMap,
        defaultValueMap
    );

    dynamicMessage.writeTo(bOut);
    bOut.close();

    //Read the written message
    defaultValueMap.clear();
    fileDescriptorMap.clear();
    typeToExtensionMap.clear();

    fin = new FileInputStream(Resources.getResource("SampleV2.desc").getPath());
    fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
    ProtobufTypeUtil.getAllFileDescriptors(fileDescriptorSet, new HashMap<>(), fileDescriptorMap);
    ProtobufTypeUtil.populateDefaultsAndExtensions(fileDescriptorMap, typeToExtensionMap, defaultValueMap);

    descriptor =
        ProtobufTypeUtil.getDescriptor(fileDescriptorSet, fileDescriptorMap, "SampleV2.desc", "util.Sample");
    DynamicMessage.Builder dynBldr = DynamicMessage.newBuilder(descriptor);
    dynBldr.mergeFrom(new ByteArrayInputStream(bOut.toByteArray()), null);

    Record record = RecordCreator.create();
    Field rootField = ProtobufTypeUtil.protobufToSdcField(record, "", descriptor, typeToExtensionMap, dynBldr.build());
    record.set(rootField);
    Assert.assertTrue(record.has("/id"));
    Assert.assertTrue(record.has("/description"));

    Assert.assertEquals(Field.Type.INTEGER, record.get("/id").getType());
    Assert.assertEquals(Field.Type.BYTE_ARRAY, record.get("/description").getType());

    Assert.assertEquals(5, record.get("/id").getValueAsInteger());
    Assert.assertArrayEquals("".getBytes(), record.get("/description").getValueAsByteArray());


  }

}
