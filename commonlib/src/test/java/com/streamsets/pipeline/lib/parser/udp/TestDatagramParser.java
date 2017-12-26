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
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDataParserFactory;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowTestUtil;
import com.streamsets.pipeline.lib.udp.UDPConstants;
import com.streamsets.pipeline.lib.util.UDPTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class TestDatagramParser {

  private static final String TEN_PACKETS = "netflow-v5-file-1";
  private static final String SINGLE_PACKET = "collectd23part.bin";
  private static final Date timestamp = new Date(1372392896000L);
  private static String SYSLOG_RFC5424;
  private static String SYSLOG_RFC3164;

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @BeforeClass
  public static void setUp() {
    String RFC3162_formatter = new SimpleDateFormat ("MMM dd HH:mm:ss", Locale.US).format(timestamp);
    DateTime datetime = new DateTime(timestamp.getTime());
    String RFC5424_formatter = datetime.toDateTimeISO().toString();

    SYSLOG_RFC5424 = "<34>1 " + RFC5424_formatter + " mymachine su: 'su root' failed for lonvick on /dev/pts/8";
    SYSLOG_RFC3164 = "<34>" + RFC3162_formatter + " mymachine su: 'su root' failed for lonvick on /dev/pts/8";
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
        .setConfig(NetflowDataParserFactory.OUTPUT_VALUES_MODE_KEY, NetflowDataParserFactory.DEFAULT_OUTPUT_VALUES_MODE)
        .setConfig(
            NetflowDataParserFactory.MAX_TEMPLATE_CACHE_SIZE_KEY,
            NetflowDataParserFactory.DEFAULT_MAX_TEMPLATE_CACHE_SIZE
        )
        .setConfig(
            NetflowDataParserFactory.TEMPLATE_CACHE_TIMEOUT_MS_KEY,
            NetflowDataParserFactory.DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS
        )
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

    NetflowTestUtil.assertRecordsForTenPackets(records);
    parser.close();
  }

  @Test
  public void testSyslogRFC3164Parser() throws Exception {
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
        UDPTestUtil.getUDPData(UDPConstants.SYSLOG, SYSLOG_RFC3164.getBytes())
    );
    DataParser parser = factory.getParser("x", byteArrayInputStream, null);

    List<Record> records = new ArrayList<>();
    Record r = parser.parse();
    while (r != null) {
      records.add(r);
      r = parser.parse();
    }

    Assert.assertEquals(1, records.size());
    Assert.assertEquals(SYSLOG_RFC3164, records.get(0).get("/raw").getValueAsString());
    Assert.assertEquals("127.0.0.1:2000", records.get(0).get("/receiverAddr").getValueAsString());
    Assert.assertEquals("127.0.0.1:3000", records.get(0).get("/senderAddr").getValueAsString());
    Assert.assertEquals("mymachine", records.get(0).get("/host").getValueAsString());
    Assert.assertEquals(2, records.get(0).get("/severity").getValueAsInteger());
    Assert.assertEquals("34", records.get(0).get("/priority").getValueAsString());
    Assert.assertEquals(4, records.get(0).get("/facility").getValueAsInteger());
    //ignore timestamp assertion test, timestamp year is not specified in RFC3162, the year will be +-1 or current year
    Assert.assertEquals(
        "su: 'su root' failed for lonvick on /dev/pts/8",
        records.get(0).get("/remaining").getValueAsString()
    );
    Assert.assertEquals(2000, records.get(0).get("/receiverPort").getValueAsInteger());
    Assert.assertEquals(3000, records.get(0).get("/senderPort").getValueAsInteger());

    parser.close();
  }

  @Test
  public void testSyslogRFC5424Parser() throws Exception {

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
        UDPTestUtil.getUDPData(UDPConstants.SYSLOG, SYSLOG_RFC5424.getBytes())
    );
    DataParser parser = factory.getParser("x", byteArrayInputStream, null);

    List<Record> records = new ArrayList<>();
    Record r = parser.parse();
    while (r != null) {
      records.add(r);
      r = parser.parse();
    }

    Assert.assertEquals(1, records.size());
    Assert.assertEquals(SYSLOG_RFC5424, records.get(0).get("/raw").getValueAsString());
    Assert.assertEquals("127.0.0.1:2000", records.get(0).get("/receiverAddr").getValueAsString());
    Assert.assertEquals("127.0.0.1:3000", records.get(0).get("/senderAddr").getValueAsString());
    Assert.assertEquals("mymachine", records.get(0).get("/host").getValueAsString());
    Assert.assertEquals(2, records.get(0).get("/severity").getValueAsInteger());
    Assert.assertEquals("34", records.get(0).get("/priority").getValueAsString());
    Assert.assertEquals(4, records.get(0).get("/facility").getValueAsInteger());
    Assert.assertEquals(timestamp, records.get(0).get("/timestamp").getValueAsDate());
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
        UDPTestUtil.getUDPData(UDPConstants.SYSLOG, SYSLOG_RFC5424.getBytes())
    );
    DataParser parser = factory.getParser("x", byteArrayInputStream, null);
    parser.parse();
  }
}
