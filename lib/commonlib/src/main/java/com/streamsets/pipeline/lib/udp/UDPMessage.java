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


import io.netty.channel.socket.DatagramPacket;

public class UDPMessage {

  private final int type;
  private final long received;
  private final DatagramPacket datagram;

  public UDPMessage(int type, long received, DatagramPacket datagram) {
    this.type = type;
    this.received = received;
    this.datagram = datagram;
  }

  public int getType() {
    return type;
  }

  public long getReceived() {
    return received;
  }

  public DatagramPacket getDatagram() {
    return datagram;
  }

  @Override
  public String toString() {
    return "UDPMessage{" +
        "type=" + type +
        ", received=" + received +
        ", datagram.sender=" + datagram.sender().getAddress().getHostAddress() + datagram.sender().getPort() +
        ", datagram.recipient=" + datagram.recipient().getAddress().getHostAddress() + datagram.recipient().getPort() +
        '}';
  }

}
