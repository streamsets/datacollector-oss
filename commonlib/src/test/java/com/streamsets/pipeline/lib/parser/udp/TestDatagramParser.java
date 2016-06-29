/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.lib.parser.udp;

import com.google.common.io.Resources;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.udp.UDPConstants;
import com.streamsets.pipeline.lib.util.UDPTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestDatagramParser {

  private static final String TEN_PACKETS = "netflow-v5-file-1";
  private static final String SINGLE_PACKET = "collectd23part.bin";
  private static final String SYSLOG = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testNetflowParser() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(
        getContext(),
        DataParserFormat.DATAGRAM
    );
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(-1)
        .setMode(DatagramMode.NETFLOW)
        .setCharset(StandardCharsets.UTF_8)
        .build();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        UDPTestUtil.getUDPData(UDPConstants.NETFLOW, Resources.toByteArray(Resources.getResource(TEN_PACKETS)))
    );
    DataParser parser = factory.getParser("x", byteArrayInputStream, null);

    List<Record> records = new ArrayList<>();
    Record r = parser.parse();
    while (r != null) {
      records.add(r);
      r = parser.parse();
    }

    UDPTestUtil.assertRecordsForTenPackets(records);
    parser.close();
  }

  @Test
  public void testSyslogParser() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(
        getContext(),
        DataParserFormat.DATAGRAM
    );
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(-1)
      .setMode(DatagramMode.SYSLOG)
      .setCharset(StandardCharsets.UTF_8)
      .build();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        UDPTestUtil.getUDPData(UDPConstants.SYSLOG, SYSLOG.getBytes())
    );
    DataParser parser = factory.getParser("x", byteArrayInputStream, null);

    List<Record> records = new ArrayList<>();
    Record r = parser.parse();
    while (r != null) {
      records.add(r);
      r = parser.parse();
    }

    Assert.assertEquals(1, records.size());
    Assert.assertEquals(SYSLOG, records.get(0).get("/raw").getValueAsString());
    Assert.assertEquals("127.0.0.1:2000", records.get(0).get("/receiverAddr").getValueAsString());
    Assert.assertEquals("127.0.0.1:3000", records.get(0).get("/senderAddr").getValueAsString());
    Assert.assertEquals("mymachine", records.get(0).get("/host").getValueAsString());
    Assert.assertEquals(2, records.get(0).get("/severity").getValueAsInteger());
    Assert.assertEquals("34", records.get(0).get("/priority").getValueAsString());
    Assert.assertEquals(4, records.get(0).get("/facility").getValueAsInteger());
    Assert.assertEquals(1444601655000L, records.get(0).get("/timestamp").getValueAsLong());
    Assert.assertEquals(
        "su: 'su root' failed for lonvick on /dev/pts/8",
        records.get(0).get("/remaining").getValueAsString()
    );
    Assert.assertEquals(2000, records.get(0).get("/receiverPort").getValueAsInteger());
    Assert.assertEquals(3000, records.get(0).get("/senderPort").getValueAsInteger());


    parser.close();
  }

  @Test
  public void testCollectdParser() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(
        getContext(),
        DataParserFormat.DATAGRAM
    );
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(-1)
      .setMode(DatagramMode.COLLECTD)
      .setCharset(StandardCharsets.UTF_8)
      .setConfig(DatagramParserFactory.EXCLUDE_INTERVAL_KEY, false)
      .build();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        UDPTestUtil.getUDPData(UDPConstants.COLLECTD, Resources.toByteArray(Resources.getResource(SINGLE_PACKET)))
    );
    DataParser parser = factory.getParser("x", byteArrayInputStream, null);

    List<Record> records = new ArrayList<>();
    Record r = parser.parse();
    while (r != null) {
      records.add(r);
      r = parser.parse();
    }
    Assert.assertEquals(23, records.size()); // 23 Value parts

    Record record0 = records.get(0);
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.expectedRecord0, record0);

    Record record2 = records.get(2);
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.expectedRecord2, record2);
  }

  @Test(expected = DataParserException.class)
  public void testUnexpectedMessageFormat() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(
      getContext(),
      DataParserFormat.DATAGRAM
    );
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(-1)
      .setMode(DatagramMode.COLLECTD)
      .setCharset(StandardCharsets.UTF_8)
      .setConfig(DatagramParserFactory.EXCLUDE_INTERVAL_KEY, false)
      .build();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
        UDPTestUtil.getUDPData(UDPConstants.SYSLOG, SYSLOG.getBytes())
    );
    DataParser parser = factory.getParser("x", byteArrayInputStream, null);
    parser.parse();
  }
}
