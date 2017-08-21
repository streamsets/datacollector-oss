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

import com.streamsets.pipeline.api.impl.Utils;
import io.netty.channel.socket.DatagramPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

// this class is not thread safe
public class UDPMessageSerializer {

  public static final int MAX_UDP_PACKAGE_SIZE = 64 * 1024;

  static class ByRefByteArrayOutputStream extends ByteArrayOutputStream {
    public byte[] getInternalBuffer() {
      return buf;
    }
  }

  private final ByRefByteArrayOutputStream baos;

  public UDPMessageSerializer() {
    baos = new ByRefByteArrayOutputStream();
  }

  public byte[] serialize(UDPMessage message) throws IOException {
    baos.reset();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeInt(UDPConstants.UDP_MESSAGE_VERSION);
    oos.writeInt(message.getType());
    oos.writeLong(message.getReceived());
    DatagramPacket datagram = message.getDatagram();
    oos.writeUTF(datagram.sender().getAddress().getHostAddress());
    oos.writeInt(datagram.sender().getPort());
    oos.writeUTF(datagram.recipient().getAddress().getHostAddress());
    oos.writeInt(datagram.recipient().getPort());
    if (datagram.content().readableBytes() > MAX_UDP_PACKAGE_SIZE) {
      throw new IOException(Utils.format("Message size '{}' exceeds maximum size '{}'",
          baos.size(),
          MAX_UDP_PACKAGE_SIZE
      ));
    }
    oos.writeInt(datagram.content().readableBytes());
    datagram.content().readBytes(oos, datagram.content().readableBytes());
    oos.close();
    byte[] buffer = new byte[baos.size()];
    System.arraycopy(baos.getInternalBuffer(), 0, buffer, 0, baos.size());
    return buffer;
  }

}
