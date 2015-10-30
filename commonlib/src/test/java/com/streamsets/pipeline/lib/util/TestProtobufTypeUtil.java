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
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestProtobufTypeUtil {

  @Test
  public void testProtobufToSdc() throws Exception {

    FileInputStream fin = new FileInputStream(Resources.getResource("Employee.desc").getPath());
    DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);

    Map<String, Set<Descriptors.FileDescriptor>> fileDescriptorDependentsMap = new HashMap<>();
    Map<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
    ProtobufTypeUtil.getAllFileDescriptors(set, fileDescriptorDependentsMap, fileDescriptorMap);

    Map<String, Set<Descriptors.FieldDescriptor>> extensionMap = ProtobufTypeUtil.getAllExtensions(fileDescriptorMap);

    Descriptors.Descriptor md = ProtobufTypeUtil.getDescriptor(set, fileDescriptorMap, "Employee.desc", "Employee");

    ExtensionRegistry extensionRegistry = ProtobufTestUtil.createExtensionRegistry(extensionMap);

    List<DynamicMessage> messages = ProtobufTestUtil.getMessages(
      md,
      extensionRegistry,
      ProtobufTestUtil.getProtoBufData()
    );

    for (int i = 0; i < messages.size(); i++) {
      DynamicMessage m = messages.get(i);

      // convert protobuf dynamic message to sdc field
      Record record = RecordCreator.create();
      Field field = ProtobufTypeUtil.protobufToSdcField(record, "", md, extensionMap, m);
      Assert.assertNotNull(field);

      // check the generated field for correctness
      ProtobufTestUtil.checkProtobufRecords(field, i);

      // check the record header for unknown fields
      ProtobufTestUtil.checkRecordForUnknownFields(record, i);
    }
  }
}
