/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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