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
package com.streamsets.pipeline.lib.parser.net.netflow;

import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Created by reading:
 * <a href="https://github.com/brockn/netflow">ASF licensed scala based netflow</a>,
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9 spec</a>,
 * and
 * <a href="http://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html#wp1003394">v1 and v5 spec</a>
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9</a>.
 */

public class NetflowDecoder extends ReplayingDecoder<Void> {
  private static final int V5_HEADER_SIZE = 24;
  private static final int V5_FLOW_SIZE = 48;

  // BEGIN ReplayingDecoder state vars
  private boolean readHeader = false;
  private int version = 0;
  private boolean readVersion = false;
  private int count = 0;
  private long uptime = 0;
  private long seconds = 0;
  private long millis = 0;
  private long timestamp = 0;
  private long flowSequence = 0;
  private short engineType = 0;
  private short engineId = 0;
  private int sampling = 0;
  private int samplingInterval = 0;
  private int samplingMode = 0;
  private UUID packetId = null;
  private String readerId = null;
  private int readIndex = 0;
  private List<NetflowMessage> result = new LinkedList<>();
  // END ReplayingDecoder state vars

  /**
   * Decodes one or more {@link NetflowMessage} from a packet.  For this method, the data (ByteBuf) is assumed
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
      List<NetflowMessage> resultMessages,
      InetSocketAddress sender,
      InetSocketAddress recipient
  ) throws OnRecordErrorException {
    final List<Object> results = new LinkedList<>();
    try {
      decode(null, buf, results, sender, recipient, true);
      for (Object result : results) {
        if (result instanceof NetflowMessage) {
          resultMessages.add((NetflowMessage) result);
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
      // 0-1
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

    switch (version) {
      case 5:
        try {
          out.addAll(parseV5(version, packetLength, packetLengthCheck, buf, sender, recipient));
        } catch (Exception e) {
          resetStateVariables();
          // this is not in a finally block, because we do NOT want to reset after each invocation, since
          // ReplayingDecoder can call this multiple times
          throw e;
        }
        break;
      default:
        resetStateVariables();
        throw new OnRecordErrorException(Errors.NETFLOW_00, version);
    }
  }

  private List<NetflowMessage> parseV5(
      int version,
      int packetLength,
      boolean packetLengthCheck,
      ByteBuf buf,
      InetSocketAddress sender,
      InetSocketAddress recipient
  ) throws OnRecordErrorException {
    if (!readHeader) {
      // 2-3
      count = buf.readUnsignedShort();
      if (count <= 0) {
        throw new OnRecordErrorException(Errors.NETFLOW_01,Utils.format("Count is invalid: {}", count));
      } else if (packetLengthCheck && packetLength < V5_HEADER_SIZE + count * V5_FLOW_SIZE) {
        String msg = Utils.format(
            "Readable bytes {} too small for count {} (max {})",
            packetLength,
            count,
            (V5_HEADER_SIZE + count * V5_FLOW_SIZE)
        );
        throw new OnRecordErrorException(Errors.NETFLOW_01, msg);
      }
      readerId = String.valueOf(recipient);
      // 4-7
      uptime = buf.readUnsignedInt();
      // 8-11
      seconds = buf.readUnsignedInt();
      millis = seconds / 1000;
      // java timestamp, which is milliseconds
      timestamp = (seconds * 1000L) + millis;
      packetId = UUIDs.startOfJavaTimestamp(timestamp);

      // 12-15
      long nanos = buf.readUnsignedInt();
      // 16-19
      flowSequence = buf.readUnsignedInt();
      // 20
      engineType = buf.readUnsignedByte();
      // 21
      engineId = buf.readUnsignedByte();
      // the first 2 bits are the sampling mode, the remaining 14 the interval
      // 22-23
      sampling = buf.readUnsignedShort();
      samplingInterval = sampling & 0x3FFF;
      samplingMode = sampling >> 14;
      readHeader = true;
      checkpoint();
    }

    while (readIndex < count) {
      //ByteBuf flowBuf = buf.slice(V5_HEADER_SIZE + (readIndex * V5_FLOW_SIZE), V5_FLOW_SIZE);

      NetflowMessage msg = new NetflowMessage();

      // 0
      int srcaddr = (int)buf.readUnsignedInt();
      // 4
      int dstaddr = (int)buf.readUnsignedInt();
      // 8
      int nexthop = (int)buf.readUnsignedInt();
      // 12
      msg.setSnmpInput(buf.readUnsignedShort());
      // 14
      msg.setSnmpOnput(buf.readUnsignedShort());
      msg.setVersion(version);

      msg.setPacketId(packetId.toString());
      msg.setSender((sender == null) ? "unknown" : sender.getAddress().toString());
      msg.setLength(packetLength);
      msg.setUptime(uptime);
      msg.setTimestamp(timestamp);
      msg.setFlowSequence(flowSequence);
      msg.setEngineId(engineId);
      msg.setEngineType(engineType);
      msg.setSamplingInterval(samplingInterval);
      msg.setSamplingMode(samplingMode);
      msg.setReaderId(readerId);

      // 16
      long packets = buf.readUnsignedInt();
      // 20
      long octets = buf.readUnsignedInt();
      // 24
      long first = buf.readUnsignedInt();
      if (first > 0) {
        msg.setFirst(timestamp - uptime - first);
      } else {
        msg.setFirst(0L);
      }

      // 28
      long last = buf.readUnsignedInt();
      if (last > 0) {
        msg.setLast(timestamp - uptime - last);
      } else {
        msg.setLast(0L);
      }

      msg.setId(UUIDs.timeBased().toString());
      msg.setSrcAddr(srcaddr);
      msg.setDstAddr(dstaddr);
      msg.setNextHop(nexthop);
      msg.setSrcAddrString(ipToString(srcaddr));
      msg.setDstAddrString(ipToString(dstaddr));
      msg.setNexthopString(ipToString(nexthop));
      // 32
      msg.setSrcPort(buf.readUnsignedShort());
      // 34
      msg.setDstPort(buf.readUnsignedShort());
      // 36 is "pad1" (unused zero bytes)
      buf.readByte();
      // 37
      msg.setTcpFlags(buf.readUnsignedByte());
      // 38
      msg.setProto(buf.readUnsignedByte());
      // 39
      msg.setTos(buf.readUnsignedByte());
      // 40
      msg.setSrcAs(buf.readUnsignedShort());
      // 42
      msg.setDstAs(buf.readUnsignedShort());
      // 44
      msg.setSrcMask(buf.readUnsignedByte());
      // 45
      msg.setDstMask(buf.readUnsignedByte());
      msg.setdPkts(packets);
      msg.setdOctets(octets);

      // 46-47 is "pad2" (unused zero bytes)
      buf.readBytes(2);
      result.add(msg);
      readIndex++;
      checkpoint();
    }

    // if we reached this point without any further Signal errors, we have finished consuming
    //checkpoint();
    LinkedList<NetflowMessage> returnResults = new LinkedList<>(result);
    resetStateVariables();
    return returnResults;
  }

  private static String ipToString(int ip) {
    return String.format("%d.%d.%d.%d",
      (ip >> 24 & 0xff),
      (ip >> 16 & 0xff),
      (ip >> 8 & 0xff),
      (ip & 0xff));
  }

  private void resetStateVariables() {
    version = 0;
    readVersion = false;
    readHeader = false;
    count = 0;
    uptime = 0;
    seconds = 0;
    millis = 0;
    timestamp = 0;
    flowSequence = 0;
    engineType = 0;
    engineId = 0;
    sampling = 0;
    samplingInterval = 0;
    samplingMode = 0;
    packetId = null;
    readerId = null;
    readIndex = 0;
    result.clear();
  }

}
