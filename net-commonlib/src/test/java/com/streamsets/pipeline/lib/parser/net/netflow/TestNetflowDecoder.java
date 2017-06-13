/**
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
package com.streamsets.pipeline.lib.parser.net.netflow;

import com.google.common.primitives.Bytes;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.net.NetTestUtils;
import com.streamsets.pipeline.sdk.RecordCreator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class TestNetflowDecoder {

  @Test
  public void testSinglePacket() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new NetflowDecoder());

    byte[] bytes = get10MessagesBytes();
    ch.writeInbound(Unpooled.wrappedBuffer(bytes));

    List<Record> records = collect10MessagesFromChannel(ch, bytes.length);

    ch.finish();

    NetflowTestUtil.assertRecordsForTenPackets(records);
  }

  @Test
  public void testMultiplePackets() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new NetflowDecoder());

    byte[] bytes = get10MessagesBytes();

    long bytesWritten = 0;
    List<List<Byte>> slices = NetTestUtils.getRandomByteSlices(bytes);
    for (int s = 0; s<slices.size(); s++) {
      List<Byte> slice = slices.get(s);
      byte[] sliceBytes = Bytes.toArray(slice);
      ch.writeInbound(Unpooled.wrappedBuffer(sliceBytes));
      bytesWritten += sliceBytes.length;
    }

    assertThat(bytesWritten, equalTo((long)bytes.length));

    List<Record> records = collect10MessagesFromChannel(ch, bytes.length);

    ch.finish();

    NetflowTestUtil.assertRecordsForTenPackets(records);
  }

  private byte[] get10MessagesBytes() throws IOException {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("com/streamsets/pipeline/lib/parser/net/netflow/netflow-v5-file-1");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    //IOUtils.copyLarge(is, baos, 0, 0);
    IOUtils.copy(is, baos);
    is.close();
    baos.close();
    return baos.toByteArray();
  }

  @NotNull
  private List<Record> collect10MessagesFromChannel(EmbeddedChannel ch, int packetLength) {
    List<Record> records = new LinkedList<>();
    for (int i=0; i<10; i++) {
      Object object = ch.readInbound();
      assertNotNull(object);
      assertThat(object, is(instanceOf(NetflowMessage.class)));
      NetflowMessage msg = (NetflowMessage) object;
      // fix packet length for test; it passes in MAX_LENGTH by default
      msg.setLength(packetLength);
      Record record = RecordCreator.create();
      msg.populateRecord(record);
      records.add(record);
    }
    return records;
  }

  @Test
  public void testTimestamps() {

    EmbeddedChannel ch = new EmbeddedChannel(new NetflowDecoder());

    final long uptime = RandomUtils.nextLong(0L, 1000000L);
    final long seconds = RandomUtils.nextLong(0L, 1500000000L);
    final long nanos = RandomUtils.nextLong(0L, 1000000000L-1L);
    NetflowTestUtil.writeV5NetflowHeader(ch, 1, uptime, seconds, nanos, 0L, 0, 0, 0);

    final long first = RandomUtils.nextLong(uptime + 1L, 2000000L);
    final long last = RandomUtils.nextLong(first + 1L, 3000000L);
    NetflowTestUtil.writeV5NetflowFlowRecord(ch, 0, 0, 0, 0, 0, 1, 1, first, last, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    Object obj = ch.readInbound();
    assertThat(obj, instanceOf(NetflowMessage.class));
    NetflowMessage msg = (NetflowMessage) obj;

    assertThat(msg.getCount(), equalTo(1));
    assertThat(msg.getSeconds(), equalTo(seconds));
    assertThat(msg.getNanos(), equalTo(nanos));
    assertThat(msg.getUptime(), equalTo(uptime));

    final long expectedTimestamp = seconds * 1000 + (nanos/1000000);
    assertThat(msg.getTimestamp(), equalTo(expectedTimestamp));

    final long expectedFirst = expectedTimestamp - uptime + first;
    assertThat(msg.getFirst(), equalTo(expectedFirst));

    final long expectedLast = expectedTimestamp - uptime + last;
    assertThat(msg.getLast(), equalTo(expectedLast));
  }
}
