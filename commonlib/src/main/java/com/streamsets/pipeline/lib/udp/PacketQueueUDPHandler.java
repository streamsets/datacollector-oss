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

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

@ChannelHandler.Sharable
public class PacketQueueUDPHandler extends SimpleChannelInboundHandler<DatagramPacket> {
  private static final Logger LOG = LoggerFactory.getLogger(PacketQueueUDPHandler.class);

  public static final String GAUGE_PACKET_QUEUE_SIZE = "Queue Size";
  public static final String GAUGE_NUM_DROPPED_PACKETS = "Dropped Packets";
  public static final String GAUGE_NUM_QUEUED_PACKETS = "Queued Packets";

  private final Map<String, Object> gaugeMap;
  private AtomicLong droppedPacketCount = new AtomicLong();
  private AtomicLong queuedPacketCount = new AtomicLong();

  private final BlockingDeque<DatagramPacket> queue;

  public PacketQueueUDPHandler(Map<String, Object> gaugeMap, int packetQueueSize) {
    this.gaugeMap = gaugeMap;

    queue = new LinkedBlockingDeque<>(packetQueueSize);
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
    packet.retain();
    final boolean succeeded = queue.offer(packet);
    if (succeeded) {
      gaugeMap.put(GAUGE_NUM_QUEUED_PACKETS, queuedPacketCount.incrementAndGet());
      gaugeMap.put(GAUGE_PACKET_QUEUE_SIZE, queue.size());
    } else {
      gaugeMap.put(GAUGE_NUM_DROPPED_PACKETS, droppedPacketCount.incrementAndGet());
      // allow Netty to collect the buffer
      packet.release();
    }
  }

  public BlockingDeque<DatagramPacket> getPacketQueue() {
    return queue;
  }
}
