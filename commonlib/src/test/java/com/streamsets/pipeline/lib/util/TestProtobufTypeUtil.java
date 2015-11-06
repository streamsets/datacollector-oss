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
package com.streamsets.pipeline.lib.util;

import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestProtobufTypeUtil {

  private Map<String, Set<Descriptors.FileDescriptor>> fileDescriptorDependentsMap = new HashMap<>();
  private Map<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
  private Map<String, Object> defaultValueMap = new HashMap<>();
  private Map<String, Set<Descriptors.FieldDescriptor>> typeToExtensionMap = new HashMap<>();
  private DescriptorProtos.FileDescriptorSet set;
  private Descriptors.Descriptor md;
  private ExtensionRegistry extensionRegistry;


  @Before
  public void setUp() throws Exception {
    FileInputStream fin = new FileInputStream(Resources.getResource("Employee.desc").getPath());
    set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
    ProtobufTypeUtil.getAllFileDescriptors(set, fileDescriptorDependentsMap, fileDescriptorMap);
    ProtobufTypeUtil.populateDefaultsAndExtensions(fileDescriptorMap, typeToExtensionMap, defaultValueMap);
    md = ProtobufTypeUtil.getDescriptor(set, fileDescriptorMap, "Employee.desc", "Employee");
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
}
