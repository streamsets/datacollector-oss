/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.udp;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class UDPConsumingServer {
  private static final Logger LOG = LoggerFactory.getLogger(UDPConsumingServer.class);
  private final List<InetSocketAddress> addresses;
  private final UDPConsumer udpConsumer;
  private final List<ChannelFuture> channelFutures;
  private EventLoopGroup group;

  static {
    // required to fully disable direct buffers which
    // while faster to allocate when shared, come with
    // unpredictable limits
    if (System.getProperty("io.netty.noUnsafe") == null) {
      System.setProperty("io.netty.noUnsafe", "true");
    }
  }

  public UDPConsumingServer(List<InetSocketAddress> addresses, UDPConsumer udpConsumer) {
    this.addresses = ImmutableList.copyOf(addresses);
    this.udpConsumer = udpConsumer;
    this.channelFutures = new ArrayList<>();
  }

  public void listen() throws Exception {
    group = new NioEventLoopGroup();
    for (SocketAddress address : addresses) {
      Bootstrap b = new Bootstrap();
      b.group(group)
        .channel(NioDatagramChannel.class)
        .handler(new UDPConsumingServerHandler(udpConsumer))
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator()); // use on-heap buffers
      LOG.info("Starting server on address {}", address);
      ChannelFuture channelFuture = b.bind(address).sync();
      channelFutures.add(channelFuture);
    }
  }

  public void destroy() {
    LOG.info("Destorying server on address(es) {}", addresses);
    for (ChannelFuture channelFuture : channelFutures) {
      if (channelFuture != null && channelFuture.isCancellable()) {
        channelFuture.cancel(true);
      }
    }
    if (group != null && !group.isShutdown() && !group.isShuttingDown()) {
      try {
        group.shutdownGracefully().get();
      } catch (InterruptedException ex) {
        // ignore
      } catch (Exception ex) {
        LOG.error("Unexpected error shutting down: " + ex, ex);
      }
    }
    group = null;
    channelFutures.clear();
  }
  public void start() {
    Utils.checkNotNull(channelFutures, "Channel future cannot be null");
    Utils.checkNotNull(group, "Event group cannot be null");
    for (ChannelFuture channelFuture : channelFutures) {
      channelFuture.channel().closeFuture();
    }
  }
}
