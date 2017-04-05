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

package com.streamsets.pipeline.stage.origin.tcp;

import com.streamsets.pipeline.lib.network.BaseNettyServer;
import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.List;

public class TCPConsumingServer extends BaseNettyServer {

  private final ChannelInitializer<SocketChannel> channelInitializer;

  public TCPConsumingServer(
      boolean enableEpoll,
      int numThreads,
      List<InetSocketAddress> addresses,
      ChannelInitializer<SocketChannel> channelInitializer
  ) {
    super(
        enableEpoll,
        numThreads,
        addresses
    );

    this.channelInitializer = channelInitializer;
  }

  @Override
  protected AbstractBootstrap bootstrap(boolean enableEpoll) {
    if (enableEpoll) {
      enableDirectBuffers();
      EventLoopGroup workerGroup = new EpollEventLoopGroup(numThreads);
      EpollEventLoopGroup bossGroup = new EpollEventLoopGroup(numThreads); // (1)
      groups.add(bossGroup);
      groups.add(workerGroup);
      ServerBootstrap b = new ServerBootstrap(); // (2)
      b.group(bossGroup, workerGroup)
          .channel(EpollServerSocketChannel.class) // (3)
          .childHandler(this.channelInitializer)
          .option(ChannelOption.SO_BACKLOG, 128)          // (5)
          .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)
      return b;
    } else {
      disableDirectBuffers();
      EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
      EventLoopGroup workerGroup = new NioEventLoopGroup();
      groups.add(bossGroup);
      groups.add(workerGroup);
      ServerBootstrap b = new ServerBootstrap(); // (2)
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class) // (3)
          .childHandler(this.channelInitializer)
          .option(ChannelOption.SO_BACKLOG, 128)          // (5)
          .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)
      return b;
    }

  }
}
