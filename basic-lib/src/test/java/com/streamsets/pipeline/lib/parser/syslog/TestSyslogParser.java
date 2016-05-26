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
package com.streamsets.pipeline.lib.parser.syslog;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class TestSyslogParser {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testRfc5424DateParsing() throws Exception {
    final String[] examples = {
      "1985-04-12T23:20:50.52Z", "1985-04-12T19:20:50.52-04:00",
      "2003-10-11T22:14:15.003Z", "2003-08-24T05:14:15.000003-07:00",
      "2012-04-13T11:11:11-08:00", "2012-04-13T08:08:08.0001+00:00",
      "2012-04-13T08:08:08.251+00:00"
    };
    SyslogParser parser = new SyslogParser(getContext(), StandardCharsets.UTF_8);
    DateTimeFormatter jodaParser = ISODateTimeFormat.dateTimeParser();
    for (String ex : examples) {
      Assert.assertEquals(
        "Problem parsing date string: " + ex,
        jodaParser.parseMillis(ex),
        parser.parseRfc5424Date("", ex));
    }
  }

  @Test
  public void testRfc5424DateParsingNoTimeZone() throws Exception {
    SyslogParser parser = new SyslogParser(getContext(), StandardCharsets.UTF_8);
    Assert.assertEquals(1458161275889L, parser.parseRfc5424Date("", "2016-03-16T20:47:55.889893")); // SDC-2605
  }

  @Test
  public void testParseFailure() throws Exception {
    SyslogParser parser = new SyslogParser(getContext(), StandardCharsets.UTF_8);
    String msg = "total junk bs";
    byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    ByteBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    try {
      parser.parse(buffer, InetSocketAddress.createUnresolved("localhost", 5000),
        InetSocketAddress.createUnresolved("localhost", 50000));
      Assert.fail("Expected OnRecordErrorException");
    } catch (OnRecordErrorException ex) {
      Record record = ex.getRecord();
      Assert.assertEquals(msg, record.get().getValueAsString());
    }
  }

  @Test
  public void testMessageParsing() throws Exception {
    SyslogParser parser = new SyslogParser(getContext(), StandardCharsets.UTF_8);
    List<String> messages = Lists.newArrayList();

    // supported examples from RFC 3164
    messages.add("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for " +
      "lonvick on /dev/pts/8");
    messages.add("<13>Feb  5 17:32:18 10.0.0.99 Use the BFG!");
    messages.add("<165>Aug 24 05:34:00 CST 1987 mymachine myproc[10]: %% " +
      "It's time to make the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # " +
      "Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport: " +
      "Conveyer1=OK, Conveyer2=OK # %%");
    messages.add("<0>Oct 22 10:52:12 scapegoat 1990 Oct 22 10:52:01 TZ-6 " +
      "scapegoat.dmz.example.org 10.1.2.3 sched[0]: That's All Folks!");

    // supported examples from RFC 5424
    messages.add("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - " +
      "ID47 - BOM'su root' failed for lonvick on /dev/pts/8");
    messages.add("<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc " +
      "8710 - - %% It's time to make the do-nuts.");

    // non-standard (but common) messages (RFC3339 dates, no version digit)
    messages.add("<13>2003-08-24T05:14:15Z localhost snarf?");
    messages.add("<13>2012-08-16T14:34:03-08:00 127.0.0.1 test shnap!");

    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    // test with default keepFields = false
    for (String msg : messages) {
      byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
      ByteBuf buffer = allocator.buffer(bytes.length);
      buffer.writeBytes(bytes);
      List<Record> records = parser.parse(buffer, InetSocketAddress.createUnresolved("localhost", 5000),
        InetSocketAddress.createUnresolved("localhost", 50000));
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("Failure to parse known-good syslog message",
        msg, records.get(0).get("/raw").getValueAsString());
      Assert.assertEquals("Failure to parse known-good syslog message",
        "localhost:5000", records.get(0).get("/receiverAddr").getValueAsString());
      Assert.assertEquals("Failure to parse known-good syslog message",
        "localhost:50000", records.get(0).get("/senderAddr").getValueAsString());
      Assert.assertNotNull("Failure to parse known-good syslog message",
        records.get(0).get("/host").getValueAsString());
    }
  }

  @Test
  public void testMessageParsingIPv6() throws Exception {
    SyslogParser parser = new SyslogParser(getContext(), StandardCharsets.UTF_8);
    List<String> messages = Lists.newArrayList();

    // supported examples from RFC 3164
    messages.add("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for " +
        "lonvick on /dev/pts/8");
    messages.add("<13>Feb  5 17:32:18 10.0.0.99 Use the BFG!");
    messages.add("<165>Aug 24 05:34:00 CST 1987 mymachine myproc[10]: %% " +
        "It's time to make the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # " +
        "Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport: " +
        "Conveyer1=OK, Conveyer2=OK # %%");
    messages.add("<0>Oct 22 10:52:12 scapegoat 1990 Oct 22 10:52:01 TZ-6 " +
        "scapegoat.dmz.example.org 10.1.2.3 sched[0]: That's All Folks!");

    // supported examples from RFC 5424
    messages.add("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - " +
        "ID47 - BOM'su root' failed for lonvick on /dev/pts/8");
    messages.add("<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc " +
        "8710 - - %% It's time to make the do-nuts.");

    // non-standard (but common) messages (RFC3339 dates, no version digit)
    messages.add("<13>2003-08-24T05:14:15Z localhost snarf?");
    messages.add("<13>2012-08-16T14:34:03-08:00 127.0.0.1 test shnap!");

    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    // test with default keepFields = false
    for (String msg : messages) {
      byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
      ByteBuf buffer = allocator.buffer(bytes.length);
      buffer.writeBytes(bytes);
      List<Record> records = parser.parse(
          buffer,
          InetSocketAddress.createUnresolved("::1", 5000),
          InetSocketAddress.createUnresolved("2001:db8::ff00:42:8329", 50000)
      );
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("Failure to parse known-good syslog message",
          msg, records.get(0).get("/raw").getValueAsString());
      Assert.assertEquals("Failure to parse known-good syslog message",
          "[::1]:5000", records.get(0).get("/receiverAddr").getValueAsString());
      Assert.assertEquals("Failure to parse known-good syslog message",
          "[2001:db8::ff00:42:8329]:50000", records.get(0).get("/senderAddr").getValueAsString());
      Assert.assertNotNull("Failure to parse known-good syslog message",
          records.get(0).get("/host").getValueAsString());
    }
  }
}