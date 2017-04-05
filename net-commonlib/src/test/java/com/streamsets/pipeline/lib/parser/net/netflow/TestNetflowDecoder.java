/*
 * Copyright 2017 StreamSets Inc.
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

package com.streamsets.pipeline.lib.parser.net.netflow;

import com.google.common.primitives.Bytes;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.net.NetTestUtils;
import com.streamsets.pipeline.sdk.RecordCreator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.io.IOUtils;
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

}
