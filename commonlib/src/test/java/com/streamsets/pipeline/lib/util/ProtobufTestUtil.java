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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.UnknownFieldSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.glassfish.jersey.internal.util.Base64;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class used by protobuf related tests.
 * Supplies test protobuf data and compares expected and actual outputs based on the supplied data
 */
public class ProtobufTestUtil {

  public static ExtensionRegistry createExtensionRegistry(Map<String, Set<Descriptors.FieldDescriptor>> extensionMap) {
    ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
    for(Map.Entry<String, Set<Descriptors.FieldDescriptor>> e : extensionMap.entrySet()) {
      Set<Descriptors.FieldDescriptor> value = e.getValue();
      for (Descriptors.FieldDescriptor f : value) {
        extensionRegistry.add(f);
      }
    }
    return extensionRegistry;
  }

  public static List<DynamicMessage> getMessages(
    Descriptors.Descriptor md,
    ExtensionRegistry extensionRegistry,
    byte[] data
  ) throws Exception {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(md);
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    List<DynamicMessage> messages = new ArrayList<>();
    while (builder.mergeDelimitedFrom(in, extensionRegistry)) {
      messages.add(builder.build());
      builder.clear();
    }
    return messages;
  }

  public static byte[] getProtoBufData() throws IOException {
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    for(int i = 0; i <10; i++) {
      getSingleProtobufData(bOut, i);
    }
    bOut.flush();
    return bOut.toByteArray();
  }

  public static void getSingleProtobufData(ByteArrayOutputStream bOut, int i) throws IOException {

    UnknownFieldSet.Field unknownIntField = UnknownFieldSet.Field.newBuilder()
      .addFixed32(1234)
      .build();
    UnknownFieldSet.Field unknownLongField = UnknownFieldSet.Field.newBuilder()
      .addFixed64(12345678)
      .build();
    UnknownFieldSet.Field unknownStringField = UnknownFieldSet.Field.newBuilder()
      .addLengthDelimited(ByteString.copyFromUtf8("Hello San FRancisco!"))
      .build();
    UnknownFieldSet.Field unknownVarIntField = UnknownFieldSet.Field.newBuilder()
      .addVarint(123456789)
      .build();

    UnknownFieldSet unknownFieldSet = UnknownFieldSet.newBuilder()
      .addField(123, unknownIntField)
      .addField(234, unknownLongField)
      .build();

    PersonProto.Person johnThePerson = PersonProto.Person.newBuilder()
      .setId(i)
      .setName("John Doe" + i)
      .setExtension(ExtensionsProto.PersonExtension.NestedPersonExtension.residenceAddress, "SJ")
      .setUnknownFields(unknownFieldSet)
      .addEmail("jdoe" + i + "@example.com")
      .addPhone(
        PersonProto.Person.PhoneNumber.newBuilder()
          .setNumber("555-4321")
          .setType(PersonProto.Person.PhoneType.HOME))
      .build();

    EmployeeProto.Employee.Builder employee;

    if(i % 2 ==0) {
      EngineerProto.Engineer johnTheEng = EngineerProto.Engineer.newBuilder()
        .setEmployeeId(String.valueOf(i))
        .setDepName("r&d")
        .setPerson(johnThePerson)
        .setExtension(ExtensionsProto.EngineerExtension.factoryAddress, "South SF")
        .build();

      employee = EmployeeProto.Employee.newBuilder()
        .setEngineer(johnTheEng)
        .setExtension(ExtensionsProto.stringField, "SF");
    } else {
      ExecutiveProto.Executive johnTheExec = ExecutiveProto.Executive.newBuilder()
        .setEmployeeId(String.valueOf(i))
        .setPerson(johnThePerson)
        .setExtension(ExtensionsProto.ExecutiveExtension.officeAddress, "SOMA")
        .build();

      employee = EmployeeProto.Employee.newBuilder()
        .setExec(johnTheExec)
        .setExtension(ExtensionsProto.stringField, "SF")
        .setExtension(ExtensionsProto.boolField, true)
        .setExtension(ExtensionsProto.intField, 4375)
        .setExtension(ExtensionsProto.doubleField, 23423.4234)
        .setExtension(ExtensionsProto.floatField, 22.22f)
        .setExtension(ExtensionsProto.bytesField, ByteString.copyFromUtf8("SanFrancisco"));
    }

    UnknownFieldSet employeeUnknownFields = UnknownFieldSet.newBuilder()
      .addField(345, unknownStringField)
      .addField(456, unknownVarIntField)
      .build();

    employee.setUnknownFields(employeeUnknownFields);
    employee.build().writeDelimitedTo(bOut);

    bOut.flush();
  }

  public static void checkProtobufRecords(Field field, int i) {
    // root field is map containing name, id, email, phone
    // name, id, email are primitive fields
    Map<String, Field> valueAsMap = field.getValueAsMap();
    Assert.assertNotNull(valueAsMap);

    Assert.assertTrue(valueAsMap.containsKey("stringField"));
    Assert.assertEquals("SF", valueAsMap.get("stringField").getValueAsString());

    if(i %2 == 0) {
      // get the nested Person object
      Assert.assertTrue(valueAsMap.containsKey("engineer"));
      valueAsMap = valueAsMap.get("engineer").getValueAsMap();
      Assert.assertNotNull(valueAsMap);
    } else {

      Assert.assertTrue(valueAsMap.containsKey("boolField"));
      Assert.assertEquals(true, valueAsMap.get("boolField").getValue());

      Assert.assertTrue(valueAsMap.containsKey("intField"));
      Assert.assertEquals(4375, valueAsMap.get("intField").getValue());

      Assert.assertTrue(valueAsMap.containsKey("doubleField"));
      Assert.assertEquals(23423.4234, valueAsMap.get("doubleField").getValue());

      Assert.assertTrue(valueAsMap.containsKey("floatField"));
      Assert.assertEquals(22.22f, valueAsMap.get("floatField").getValue());

      Assert.assertTrue(valueAsMap.containsKey("bytesField"));
      Assert.assertTrue(Arrays.equals("SanFrancisco".getBytes(), valueAsMap.get("bytesField").getValueAsByteArray()));

      Assert.assertTrue(valueAsMap.containsKey("exec"));
      valueAsMap = valueAsMap.get("exec").getValueAsMap();
      Assert.assertNotNull(valueAsMap);

    }

    Assert.assertTrue(valueAsMap.containsKey("employeeId"));
    Assert.assertEquals(String.valueOf(i), valueAsMap.get("employeeId").getValueAsString());

    if(i % 2 == 0) {
      Assert.assertTrue(valueAsMap.containsKey("depName"));
      Assert.assertEquals("r&d", valueAsMap.get("depName").getValueAsString());

      Assert.assertTrue(valueAsMap.containsKey("depid"));
      Assert.assertEquals(null, valueAsMap.get("depid").getValue());

      //engineer extension
      Assert.assertTrue(valueAsMap.containsKey("factoryAddress"));
      Assert.assertEquals("South SF", valueAsMap.get("factoryAddress").getValue());

    } else {
      //executive extension
      Assert.assertTrue(valueAsMap.containsKey("officeAddress"));
      Assert.assertEquals("SOMA", valueAsMap.get("officeAddress").getValue());

    }

    // check the person nested object
    Assert.assertTrue(valueAsMap.containsKey("person"));
    valueAsMap = valueAsMap.get("person").getValueAsMap();
    Assert.assertNotNull(valueAsMap);

    Assert.assertTrue(valueAsMap.containsKey("name"));
    Assert.assertEquals("John Doe" + i, valueAsMap.get("name").getValueAsString());

    Assert.assertTrue(valueAsMap.containsKey("name"));
    Assert.assertEquals("John Doe" + i, valueAsMap.get("name").getValueAsString());
    Assert.assertTrue(valueAsMap.containsKey("id"));
    Assert.assertEquals(i, valueAsMap.get("id").getValueAsInteger());
    Assert.assertTrue(valueAsMap.containsKey("email"));
    List<Field> email = valueAsMap.get("email").getValueAsList();
    Assert.assertEquals(1, email.size());
    Assert.assertEquals("jdoe" + i + "@example.com", email.get(0).getValueAsString());

    //Extension to Person
    Assert.assertTrue(valueAsMap.containsKey("residenceAddress"));
    Assert.assertEquals("SJ", valueAsMap.get("residenceAddress").getValueAsString());

    // phone is a repeated message [List of Map expected]
    Assert.assertTrue(valueAsMap.containsKey("phone"));
    List<Field> phone = valueAsMap.get("phone").getValueAsList();
    Assert.assertEquals(1, phone.size());
    valueAsMap = phone.get(0).getValueAsMap();
    Assert.assertEquals(2, valueAsMap.size());

    // map contains 2 keys number and type of type String
    Assert.assertTrue(valueAsMap.containsKey("number"));
    Assert.assertEquals("555-4321", valueAsMap.get("number").getValueAsString());
    Assert.assertTrue(valueAsMap.containsKey("type"));
    Assert.assertEquals("HOME", valueAsMap.get("type").getValueAsString());

  }

  public static void checkRecordForUnknownFields(Record record, int i) throws IOException {
    // unknown fields are expected in paths for person and employee
    String attribute = record.getHeader().getAttribute(ProtobufTypeUtil.PROTOBUF_UNKNOWN_FIELDS_PREFIX + "/");
    UnknownFieldSet.Builder builder = UnknownFieldSet.newBuilder();
    builder.mergeDelimitedFrom(new ByteArrayInputStream(Base64.decode(attribute.getBytes())));
    UnknownFieldSet unknownFieldSet = builder.build();
    Map<Integer, UnknownFieldSet.Field> integerFieldMap = unknownFieldSet.asMap();
    Assert.assertEquals(2, integerFieldMap.size());
    Assert.assertTrue(integerFieldMap.containsKey(345));
    Assert.assertEquals(1, integerFieldMap.get(345).getLengthDelimitedList().size());
    Assert.assertEquals(
      integerFieldMap.get(345).getLengthDelimitedList().get(0),
      ByteString.copyFromUtf8("Hello San FRancisco!")
    );
    Assert.assertTrue(integerFieldMap.containsKey(456));
    Assert.assertEquals(1, integerFieldMap.get(456).getVarintList().size());
    Assert.assertEquals(123456789, (long)integerFieldMap.get(456).getVarintList().get(0));

    if(i%2 == 0) {
      attribute = record.getHeader().getAttribute(ProtobufTypeUtil.PROTOBUF_UNKNOWN_FIELDS_PREFIX + "/engineer/person");
    } else {
      attribute = record.getHeader().getAttribute(ProtobufTypeUtil.PROTOBUF_UNKNOWN_FIELDS_PREFIX + "/exec/person");
    }
    builder = UnknownFieldSet.newBuilder();
    builder.mergeDelimitedFrom(new ByteArrayInputStream(Base64.decode(attribute.getBytes())));
    unknownFieldSet = builder.build();
    integerFieldMap = unknownFieldSet.asMap();
    Assert.assertEquals(2, integerFieldMap.size());
    Assert.assertTrue(integerFieldMap.containsKey(123));
    Assert.assertEquals(1, integerFieldMap.get(123).getFixed32List().size());
    Assert.assertEquals(1234, (int)integerFieldMap.get(123).getFixed32List().get(0));
    Assert.assertTrue(integerFieldMap.containsKey(234));
    Assert.assertEquals(1, integerFieldMap.get(234).getFixed64List().size());
    Assert.assertEquals(12345678, (long)integerFieldMap.get(234).getFixed64List().get(0));
  }

  public static void compareProtoRecords(List<Record> records, int offset) throws IOException {
    for(int i = 0; i < records.size(); i++) {
      int dataSuffix = offset + i;
      Map<String, Field> valueAsMap = records.get(i).get().getValueAsMap();
      Assert.assertNotNull(valueAsMap);
      checkProtobufRecords(records.get(i).get(), dataSuffix);
      checkRecordForUnknownFields(records.get(i), dataSuffix);
    }
  }

  // generator related
  public static List<Record> getProtobufRecords() {
    List<Record> records = new ArrayList<>();
    for(int i = 0; i < 10; i++) {
      Record r = RecordCreator.create();

      // create Person field
      Field name = Field.create("John Doe" + i);
      Field id = Field.create(i);
      Field email = Field.create("jdoe" + i + "@example.com");
      Field emailList = Field.create(Arrays.asList(email));

      Field number = Field.create("555-4321");
      Field type = Field.create("HOME");
      Map<String, Field> phone = new HashMap<>();
      phone.put("number", number);
      phone.put("type", type);
      Field phoneField = Field.create(phone);
      Field phoneList = Field.create(Arrays.asList(phoneField));

      Map<String, Field> person = new HashMap<>();
      person.put("name", name);
      person.put("id", id);
      person.put("email", emailList);
      person.put("phone", phoneList);

      Field personField = Field.create(person);

      Map<String, Field> employee = new HashMap<>();

      if (i % 2 == 0) {
        // create Engineer field
        Map<String, Field> engineer = new HashMap<>();
        engineer.put("employeeId", Field.create(String.valueOf(i)));
        engineer.put("depName", Field.create("r&d"));
        engineer.put("person", personField);

        Field engineerField = Field.create(engineer);
        employee.put("engineer", engineerField);
        employee.put("exec", Field.create(Field.Type.MAP, null));

      } else {
        // create Exec field
        Map<String, Field> exec = new HashMap<>();
        exec.put("employeeId", Field.create(String.valueOf(i)));
        exec.put("person", personField);

        Field exedcField = Field.create(exec);
        employee.put("exec", exedcField);
        employee.put("engineer", Field.create(Field.Type.MAP, null));
      }
      r.set(Field.create(employee));
      records.add(r);
    }
    return records;
  }
}
