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

import com.streamsets.pipeline.lib.network.BaseNettyServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.net.InetSocketAddress;
import java.util.List;

public class UDPConsumingServer extends BaseNettyServer {

  private final SimpleChannelInboundHandler<DatagramPacket> handler;

  public UDPConsumingServer(
      boolean enableEpoll,
      int numThreads,
      List<InetSocketAddress> addresses,
      UDPConsumer udpConsumer
  ) {
    this(
        enableEpoll,
        numThreads,
        addresses,
        new UDPConsumingServerHandler(udpConsumer)
    );
  }

  public UDPConsumingServer(
      boolean enableEpoll,
      int numThreads,
      List<InetSocketAddress> addresses,
      SimpleChannelInboundHandler<DatagramPacket> handler
  ) {
    super(
        enableEpoll,
        numThreads,
        addresses
    );
    this.handler = handler;
  }

  @Override
  protected Bootstrap bootstrap(boolean enableEpoll) {
    if (enableEpoll) {
      // Direct buffers required for Epoll
      enableDirectBuffers();
      EventLoopGroup group = new EpollEventLoopGroup(numThreads);
      groups.add(group);
      return new Bootstrap()
          .group(group)
          .channel(EpollDatagramChannel.class)
          .handler(handler)
          .option(EpollChannelOption.SO_REUSEADDR, true)
          .option(EpollChannelOption.SO_REUSEPORT, true)
          .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    } else {
      disableDirectBuffers();
      EventLoopGroup group = new NioEventLoopGroup(numThreads);
      groups.add(group);
      return new Bootstrap()
          .group(group)
          .channel(NioDatagramChannel.class)
          .handler(handler)
          .option(ChannelOption.SO_REUSEADDR, true)
          .option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator()); // use on-heap buffers
    }
  }
}
