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
package com.streamsets.pipeline.lib.network;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseNettyServer {
  private static final Logger LOG = LoggerFactory.getLogger(BaseNettyServer.class);
  protected static final String NETTY_UNSAFE = "io.netty.noUnsafe";
  protected final boolean enableEpoll;
  protected final int numThreads;
  protected final List<InetSocketAddress> addresses;
  protected final List<ChannelFuture> channelFutures = new ArrayList<>();
  protected final List<EventLoopGroup> groups = new ArrayList<>();

  private static boolean directBuffersDisabled = false;
  private static boolean directBuffersEnabled = false;

  public BaseNettyServer(
      boolean enableEpoll,
      int numThreads,
      List<InetSocketAddress> addresses
  ) {
    this.enableEpoll = enableEpoll;
    this.numThreads = numThreads;
    this.addresses = ImmutableList.copyOf(addresses);
  }

  public void listen() throws Exception {
    for (SocketAddress address : addresses) {
      AbstractBootstrap b = bootstrap(enableEpoll);
      LOG.info("Starting server on address {}", address);
      if (enableEpoll) {
        // for epoll, bind for each thread
        for (int i = 0; i < numThreads; i++) {
          ChannelFuture channelFuture = b.bind(address).sync();
          channelFutures.add(channelFuture);
        }
      } else {
        // for non-epoll (NIO threadpool), bind once
        ChannelFuture channelFuture = b.bind(address).sync();
        channelFutures.add(channelFuture);
      }
    }
  }

  protected abstract AbstractBootstrap bootstrap(boolean enableEpoll);

  public void close() {
    LOG.info("Closing server channels");
    for (ChannelFuture channelFuture : channelFutures) {
      if (channelFuture != null) {
        channelFuture.channel().disconnect().awaitUninterruptibly();
        channelFuture.channel().close().awaitUninterruptibly();
      }
    }

    shutdownGroups();
  }

  public void destroy() {
    LOG.info("Destroying server on address(es) {}", addresses);
    for (ChannelFuture channelFuture : channelFutures) {
      if (channelFuture != null && channelFuture.isCancellable()) {
        channelFuture.cancel(true);
      }
    }
    try {
      shutdownGroups();
    } finally {
      channelFutures.clear();
    }
  }

  public void start() {
    Utils.checkNotNull(channelFutures, "Channel future cannot be null");
    Utils.checkState(!groups.isEmpty(), "Event group cannot be null");
    for (ChannelFuture channelFuture : channelFutures) {
      channelFuture.channel().closeFuture();
    }
  }

  private void shutdownGroups() {
    for (EventLoopGroup group : groups) {
      if (group != null && !group.isShutdown() && !group.isShuttingDown()) {
        try {
          group.shutdownGracefully().get();
        } catch (InterruptedException ex) {
          LOG.error("InterruptedException thrown while shutting down: " + ex, ex);
          Thread.currentThread().interrupt();
        } catch (Exception ex) {
          LOG.error("Unexpected error shutting down: " + ex, ex);
        }
      }
    }
  }

  protected static void disableDirectBuffers() {
    // required to fully disable direct buffers which
    // while faster to allocate when shared, come with
    // unpredictable limits
    if (directBuffersEnabled) {
      LOG.warn("Calling disableDirectBuffers, but Netty direct buffers were previously enabled");
    }
    LOG.info("Disabling Netty direct buffers");
    directBuffersDisabled = true;
    System.setProperty(NETTY_UNSAFE, "true");
  }

  protected static void enableDirectBuffers() {
    if (directBuffersDisabled) {
      LOG.warn("Calling enableDirectBuffers, but Netty direct buffers were previously disabled");
    }
    LOG.info("Enabling Netty direct buffers");
    directBuffersEnabled = true;
    System.setProperty(NETTY_UNSAFE, "false");
  }
}
