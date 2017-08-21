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
package com.streamsets.pipeline.lib.udp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TestUDPMessageSerialization {

  private static UDPMessage createUDPMessage(int size) {
    InetSocketAddress recipient = new InetSocketAddress("127.0.0.1", 2000);
    InetSocketAddress sender = new InetSocketAddress("127.0.0.1", 3000);
    byte[] arr = new byte[size];
    for (int i = 0; i < size; i++) {
      arr[i] = (byte)(i % 256);
    }
    ByteBuf buffer = Unpooled.wrappedBuffer(arr);
    DatagramPacket datagram = new DatagramPacket(buffer, recipient, sender);
    return new UDPMessage(UDPConstants.NETFLOW, 1, datagram);
  }

  @Test
  public void testSerDeser() throws IOException {
    UDPMessage message = createUDPMessage(100);
    UDPMessageSerializer serializer = new UDPMessageSerializer();
    byte[] serData = serializer.serialize(message);

    UDPMessageDeserializer deserializer = new UDPMessageDeserializer();
    UDPMessage got = deserializer.deserialize(serData);

    Assert.assertEquals(message.getType(), got.getType());
    Assert.assertEquals(message.getReceived(), got.getReceived());
    Assert.assertEquals(message.getDatagram().sender(), got.getDatagram().sender());
    Assert.assertEquals(message.getDatagram().recipient(), got.getDatagram().recipient());
    Assert.assertArrayEquals(message.getDatagram().content().array(), got.getDatagram().content().array());
  }

}
