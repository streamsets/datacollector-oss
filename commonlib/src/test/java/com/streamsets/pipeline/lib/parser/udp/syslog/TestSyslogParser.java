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
package com.streamsets.pipeline.lib.parser.udp.syslog;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogBaseTestClass;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class TestSyslogParser extends SyslogBaseTestClass {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParseFailure() throws Exception {
    SyslogParser parser = new SyslogParser(getContext(), StandardCharsets.UTF_8);
    String msg = "<123>                    ";
    byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    ByteBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    try {
      parser.parse(
          buffer,
          InetSocketAddress.createUnresolved("localhost", 5000),
          InetSocketAddress.createUnresolved("localhost", 50000)
      );
      Assert.fail("Expected OnRecordErrorException");
    } catch (OnRecordErrorException ex) {
      Record record = ex.getRecord();
      Assert.assertEquals(msg, record.get().getValueAsString());
    }
  }

  @Test
  public void testMessageParsing() throws Exception {
    SyslogParser parser = new SyslogParser(getContext(), StandardCharsets.UTF_8);
    List<String> messages = getTestMessageStrings();

    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    // test with default keepFields = false
    for (String msg : messages) {
      byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
      ByteBuf buffer = allocator.buffer(bytes.length);
      buffer.writeBytes(bytes);
      List<Record> records = parser.parse(
          buffer,
          InetSocketAddress.createUnresolved("localhost", 5000),
          InetSocketAddress.createUnresolved("localhost", 50000)
      );
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
    List<String> messages = getTestMessageStrings();

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
