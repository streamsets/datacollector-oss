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
package com.streamsets.pipeline.lib.parser.net.syslog;

import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class TestSyslogDecoder extends SyslogBaseTestClass {

  @Test
  public void testRfc5424DateParsing() throws Exception {
    final String[] examples = {
        "1985-04-12T23:20:50.52Z",
        "1985-04-12T19:20:50.52-04:00",
        "2003-10-11T22:14:15.003Z",
        "2003-08-24T05:14:15.000003-07:00",
        "2012-04-13T11:11:11-08:00",
        "2012-04-13T08:08:08.0001+00:00",
        "2012-04-13T08:08:08.251+00:00"
    };
    SyslogDecoder decoder = new SyslogDecoder(StandardCharsets.UTF_8, getSystemClock());
    DateTimeFormatter parser = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    for (String example : examples) {
      assertEquals(
          "Problem parsing date string: " + example, OffsetDateTime.parse(example, parser).toInstant().toEpochMilli(),
          decoder.parseRfc5424Date(example)
      );
    }
  }

  @Test
  public void testRfc5424DateParsingNoTimeZone() throws Exception {
    SyslogDecoder parser = new SyslogDecoder(StandardCharsets.UTF_8, getSystemClock());
    assertEquals(1458161275889L, parser.parseRfc5424Date("2016-03-16T20:47:55.889893")); // SDC-2605
  }

  @Test
  public void testDecodeMessages() throws Exception {
    SyslogDecoder decoder = new SyslogDecoder(StandardCharsets.UTF_8, getSystemClock());
    for (Map.Entry<String, SyslogMessage> entry : getTestMessageRawToExpected().entrySet()) {
      final List<Object> outputs = new LinkedList<>();
      decoder.decode(
          null,
          Unpooled.copiedBuffer(entry.getKey(), StandardCharsets.UTF_8),
          outputs,
          getDummyReceiver(),
          getDummySender()
      );
      assertThat(outputs.size(), equalTo(1));
      assertThat(outputs.get(0), instanceOf(SyslogMessage.class));
      SyslogMessage msg = (SyslogMessage) outputs.get(0);
      final SyslogMessage exp = entry.getValue();

      assertThat(msg.getSenderAddress(), equalTo(exp.getSenderAddress()));
      assertThat(msg.getSenderHost(), equalTo(exp.getSenderHost()));
      assertThat(msg.getSenderPort(), equalTo(exp.getSenderPort()));
      assertThat(msg.getReceiverAddress(), equalTo(exp.getReceiverAddress()));
      assertThat(msg.getReceiverHost(), equalTo(exp.getReceiverHost()));
      assertThat(msg.getReceiverPort(), equalTo(exp.getReceiverPort()));

      assertThat(msg.getHost(), equalTo(exp.getHost()));
      assertThat(msg.getRawMessage(), equalTo(exp.getRawMessage()));
      assertThat(msg.getFacility(), equalTo(exp.getFacility()));
      assertThat(msg.getPriority(), equalTo(exp.getPriority()));
      assertThat(msg.getSeverity(), equalTo(exp.getSeverity()));
      assertThat(msg.getTimestamp(), equalTo(exp.getTimestamp()));
      assertThat(msg.getRemainingMessage(), equalTo(exp.getRemainingMessage()));
    }
  }
}
