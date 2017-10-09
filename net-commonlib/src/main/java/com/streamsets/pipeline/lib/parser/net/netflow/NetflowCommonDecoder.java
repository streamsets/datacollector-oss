/**
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
package com.streamsets.pipeline.lib.parser.net.netflow;

import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.net.netflow.v5.NetflowV5Decoder;
import com.streamsets.pipeline.lib.parser.net.netflow.v5.NetflowV5Message;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.NetflowV9Decoder;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.NetflowV9TemplateCacheProvider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by reading:
 * <a href="https://github.com/brockn/netflow">ASF licensed scala based netflow</a>,
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9 spec</a>,
 * and
 * <a href="http://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html#wp1003394">v1 and v5 spec</a>
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9</a>.
 */

public class NetflowCommonDecoder extends ReplayingDecoder<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(NetflowCommonDecoder.class);

  // BEGIN ReplayingDecoder state vars
  private int version = 0;
  private boolean readVersion = false;
  private NetflowV5Decoder netflowV5Decoder;
  private NetflowV9Decoder netflowV9Decoder;
  // END ReplayingDecoder state vars

  // Netflow v9 decoder parameters
  private final OutputValuesMode outputValuesMode;
  private final NetflowV9TemplateCacheProvider templateCacheProvider;

  public NetflowCommonDecoder(
      OutputValuesMode outputValuesMode,
      int maxTemplateCacheSize,
      int templateCacheTimeoutMs
  ) {
    this(outputValuesMode, () -> NetflowV9Decoder.buildTemplateCache(maxTemplateCacheSize, templateCacheTimeoutMs));
  }

  public NetflowCommonDecoder(
      OutputValuesMode outputValuesMode,
      NetflowV9TemplateCacheProvider templateCacheProvider
  ) {
    this.outputValuesMode = outputValuesMode;
    this.templateCacheProvider = templateCacheProvider;
  }

  /**
   * Decodes one or more {@link NetflowV5Message} from a packet.  For this method, the data (ByteBuf) is assumed
   * to be complete (i.e. it will contain all the data), which is the case for UDP.
   *
   * @param buf the byte buffer from the packet
   * @param resultMessages a list of messages to populate from the parsing operation
   * @param sender the packet sender address
   * @param recipient the packet recipient address
   * @throws OnRecordErrorException
   */
  public void decodeStandaloneBuffer(
      ByteBuf buf,
      List<BaseNetflowMessage> resultMessages,
      InetSocketAddress sender,
      InetSocketAddress recipient
  ) throws OnRecordErrorException {
    final List<Object> results = new LinkedList<>();
    try {
      decode(null, buf, results, sender, recipient, true);
      for (Object result : results) {
        if (result == null) {
          LOG.warn("null result found from decoding standalone Netflow buffer; skipping");
          continue;
        }
        if (result instanceof BaseNetflowMessage) {
          resultMessages.add((BaseNetflowMessage) result);
        } else {
          throw new IllegalStateException(String.format(
              "Found unexpected object type in results: %s",
              result.getClass().getName())
          );
        }
      }
    } finally {
      resetStateVariables();
    }
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
    decode(ctx, buf, out, null, null, false);
  }

  protected void decode(
      ChannelHandlerContext ctx,
      ByteBuf buf,
      List<Object> out,
      InetSocketAddress sender,
      InetSocketAddress recipient,
      boolean packetLengthCheck
  ) throws OnRecordErrorException {
    int packetLength = buf.readableBytes();
    if (!readVersion) {
      // 0-1, for both Netflow 5 and 9
      version = buf.readUnsignedShort();
      readVersion = true;
      checkpoint();
    }

    if (ctx != null) {
      if (sender == null) {
        SocketAddress socketAddress = ctx.channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress) {
          sender = (InetSocketAddress) socketAddress;
        }
      }
      if (recipient == null) {
        SocketAddress socketAddress = ctx.channel().localAddress();
        if (socketAddress instanceof InetSocketAddress) {
          recipient = (InetSocketAddress) socketAddress;
        }
      }
    }

    // lazy instantiation of the version specific decoder, but use the
    VersionSpecificNetflowDecoder<?> versionSpecificNetflowDecoder;
    switch (version) {
      case 5:
        if (netflowV5Decoder == null) {
          netflowV5Decoder = new NetflowV5Decoder(this);
        }
        versionSpecificNetflowDecoder = netflowV5Decoder;
        break;
      case 9:
        if (netflowV9Decoder == null) {
          // lazy instantiation of the version specific decoder
          netflowV9Decoder = new NetflowV9Decoder(this, outputValuesMode, templateCacheProvider);
        }
        versionSpecificNetflowDecoder = netflowV9Decoder;
        break;
      default:
        resetStateVariables();
        throw new OnRecordErrorException(Errors.NETFLOW_00, version);
    }

    try {
      out.addAll(versionSpecificNetflowDecoder.parse(version, packetLength, packetLengthCheck, buf, sender, recipient));
    } catch (Exception e) {
      resetStateVariables();
      // this is not in a finally block, because we do NOT want to reset after each invocation, since
      // ReplayingDecoder can call this multiple times
      throw e;
    }
  }

  public static String ipV4ToString(int ip) {
    return String.format("%d.%d.%d.%d",
      (ip >> 24 & 0xff),
      (ip >> 16 & 0xff),
      (ip >> 8 & 0xff),
      (ip & 0xff));
  }

  @NotNull
  public static String getIpV4Address(byte[] rawBytes) throws OnRecordErrorException {
    try {
      final String ipV4Addr = InetAddress.getByAddress(rawBytes).getHostAddress();
      return ipV4Addr;
    } catch (UnknownHostException e) {
      throw new OnRecordErrorException(Errors.NETFLOW_14, Arrays.toString(rawBytes), e.getMessage(), e);
    }
  }

  private void resetStateVariables() {
    version = 0;
    readVersion = false;
    if (netflowV5Decoder != null) {
      netflowV5Decoder.resetState();
    }
    if (netflowV9Decoder != null) {
      netflowV9Decoder.resetState();
    }
  }

  /**
   * public wrapper method for {@link #checkpoint()}, for the purpose of allowing the child decoders to record their
   * checkpoints as they go, since the parent decoder (i.e. this) is solely responsible for maintaining ByteBuf
   * position as far as Netty is concerned
   */
  public void doCheckpoint() {
    checkpoint();
  }
}
