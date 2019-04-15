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
package com.streamsets.pipeline.stage.origin.tcp;

import com.streamsets.pipeline.lib.network.BaseNettyServer;
import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.List;

public class TCPConsumingServer extends BaseNettyServer {

  private static final int NUM_BOSS_THREADS = 1;

  // TODO: make config options?
  private static final int SOCKET_MAX_INBOUND_CONNECTION_QUEUE_DEPTH = 128;
  private static final boolean SOCKET_KEEPALIVE = true;

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
      // boss group simply opens channels and hands processing off to the child
      EpollEventLoopGroup bossGroup = new EpollEventLoopGroup(NUM_BOSS_THREADS);
      EventLoopGroup workerGroup = new EpollEventLoopGroup(numThreads);
      groups.add(bossGroup);
      groups.add(workerGroup);
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(EpollServerSocketChannel.class)
          .childHandler(this.channelInitializer)
          .option(EpollChannelOption.SO_REUSEADDR, true)
          .option(EpollChannelOption.SO_REUSEPORT, true)
          .option(ChannelOption.SO_BACKLOG, SOCKET_MAX_INBOUND_CONNECTION_QUEUE_DEPTH)
          .childOption(ChannelOption.SO_KEEPALIVE, SOCKET_KEEPALIVE);
      return b;
    } else {
      disableDirectBuffers();
      EventLoopGroup bossGroup = new NioEventLoopGroup(NUM_BOSS_THREADS);
      EventLoopGroup workerGroup = new NioEventLoopGroup(numThreads);
      groups.add(bossGroup);
      groups.add(workerGroup);
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(this.channelInitializer)
          .option(ChannelOption.SO_BACKLOG, SOCKET_MAX_INBOUND_CONNECTION_QUEUE_DEPTH)
          .childOption(ChannelOption.SO_KEEPALIVE, SOCKET_KEEPALIVE);
      return b;
    }

  }
}
