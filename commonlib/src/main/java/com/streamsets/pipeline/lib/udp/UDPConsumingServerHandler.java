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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class UDPConsumingServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {
  private static final Logger LOG = LoggerFactory.getLogger(UDPConsumingServerHandler.class);
  private final UDPConsumer udpConsumer;

  public UDPConsumingServerHandler(UDPConsumer udpConsumer) {
    this.udpConsumer = udpConsumer;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    String msg = "Uncaught throwable in UDP Server: " + cause;
    LOG.error(msg, cause);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
    udpConsumer.process(packet);
  }
}
