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
package com.streamsets.pipeline.lib.generator.protobuf;

import com.google.common.io.Resources;
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
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class TestProtobufDataGenerator {

  @Test
  public void testProtobufDataGenerator() throws Exception {
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();

    // create mock sdc records that mimick records parsed from protobuf represented by Employee.desc file.
    List<Record> records = ProtobufTestUtil.getProtobufRecords();

    // write these records using the protobuf generator
    DataGenerator gen = getDataGenerator(bOut, "Employee.desc", "Employee");
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

  public DataGenerator getDataGenerator(OutputStream os, String protoFile, String messageType)
      throws IOException, DataParserException {
    return getDataGeneratorFactory(protoFile, messageType).getGenerator(os);
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  public DataGeneratorFactory getDataGeneratorFactory(String protoFile, String messageType) {
    DataGeneratorFactoryBuilder dataGenFactoryBuilder = new DataGeneratorFactoryBuilder(getContext(),
      DataGeneratorFormat.PROTOBUF);
    DataGeneratorFactory factory = dataGenFactoryBuilder
      .setConfig(ProtobufConstants.PROTO_DESCRIPTOR_FILE_KEY, Resources.getResource(protoFile).getPath())
      .setConfig(ProtobufConstants.MESSAGE_TYPE_KEY, messageType)
      .build();
    return factory;
  }

}
