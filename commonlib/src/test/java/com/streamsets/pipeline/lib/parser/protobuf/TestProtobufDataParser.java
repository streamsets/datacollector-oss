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
package com.streamsets.pipeline.lib.parser.protobuf;

import com.google.common.io.Resources;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.parser.Errors;
import com.streamsets.pipeline.lib.util.PersonProto;
import com.streamsets.pipeline.lib.util.ProtobufConstants;
import com.streamsets.pipeline.lib.util.ProtobufTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestProtobufDataParser {

  @Test
  public void testProtobufDataParser() throws IOException, DataParserException {
    DataParser dataParser = getDataParser("0", "Employee.desc", "Employee");
    List<Record> records = new ArrayList<>();
    Record r = dataParser.parse();
    // Engineer object in the stream is 59 bytes long
    Assert.assertEquals("125", dataParser.getOffset());
    while(r != null) {
      records.add(r);
      r = dataParser.parse();
    }
    Assert.assertTrue("-1".equals(dataParser.getOffset()));
    Assert.assertEquals(10, records.size());
    ProtobufTestUtil.compareProtoRecords(records, 0);
  }

  @Test
  public void testProtobufDataParserWithOffset() throws IOException, DataParserException {
    // Engineer object in the stream is 59 bytes long
    // Executive object in the stream is 54 bytes long
    // Skip first 5 objects => start from position 285 [59*3 54*2]
    DataParser dataParser = getDataParser("685", "Employee.desc", "Employee");
    List<Record> records = new ArrayList<>();
    Assert.assertEquals("685", dataParser.getOffset());
    Record r = dataParser.parse();
    Assert.assertEquals("840", dataParser.getOffset());
    while(r != null) {
      records.add(r);
      r = dataParser.parse();
    }
    Assert.assertTrue("-1".equals(dataParser.getOffset()));
    Assert.assertEquals(5, records.size());
    ProtobufTestUtil.compareProtoRecords(records, 5);
  }

  @Test
  public void testMissingRequiredField() throws Exception {
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    PersonProto.Person john =
      PersonProto.Person.newBuilder()
        .setId(1)
        .setName("John Doe")
        .addEmail("jdoe" + "@example.com")
        .addPhone(
          PersonProto.Person.PhoneNumber.newBuilder()
            .setNumber("7568345")
        ).build();
    john.writeDelimitedTo(bOut);
    bOut.flush();

    DataParserFactory factory = getDataParserFactory("test1.desc", "Person");
    DataParser parser = factory.getParser("Person", new ByteArrayInputStream(bOut.toByteArray()), "0");

    try {
      parser.parse();
      Assert.fail("DataParserException expected as a required field is missing");
    } catch (DataParserException e) {
      Assert.assertEquals(Errors.DATA_PARSER_02, e.getErrorCode());
    }
  }

  public DataParser getDataParser(String offset, String protoFile, String messageType) throws IOException, DataParserException {
    return getDataParserFactory(protoFile, messageType)
        .getParser(
          "Person",
          new ByteArrayInputStream(ProtobufTestUtil.getProtoBufData()),
          offset
        );
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  public DataParserFactory getDataParserFactory(String protoFile, String messageType) {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
      DataParserFormat.PROTOBUF);
    DataParserFactory factory = dataParserFactoryBuilder
      .setConfig(ProtobufConstants.PROTO_DESCRIPTOR_FILE_KEY, Resources.getResource(protoFile).getPath())
      .setConfig(ProtobufConstants.MESSAGE_TYPE_KEY, messageType)
      .setOverRunLimit(1000)
      .setMaxDataLen(Integer.MAX_VALUE)
      .build();
    return factory;
  }
}
