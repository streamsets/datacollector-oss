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

package com.streamsets.pipeline.lib.parser.net.netflow.v5;

import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.net.netflow.Errors;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowCommonDecoder;
import com.streamsets.pipeline.lib.parser.net.netflow.UUIDs;
import com.streamsets.pipeline.lib.parser.net.netflow.VersionSpecificNetflowDecoder;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class NetflowV5Decoder implements VersionSpecificNetflowDecoder<NetflowV5Message> {
  private static final int V5_HEADER_SIZE = 24;
  private static final int V5_FLOW_SIZE = 48;

  private final NetflowCommonDecoder parentDecoder;

  // BEGIN state vars
  private boolean readHeader = false;
  private int count = 0;
  private long uptime = 0;
  private long seconds = 0;
  private long nanos = 0;
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
  private List<NetflowV5Message> result = new LinkedList<>();
  // END state vars

  public NetflowV5Decoder(NetflowCommonDecoder parentDecoder) {
    this.parentDecoder = parentDecoder;
  }

  @Override
  public List<NetflowV5Message> parse(
      int netflowVersion,
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
        throw new OnRecordErrorException(Errors.NETFLOW_01, Utils.format("Count is invalid: {}", count));
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
      // 12-15
      nanos = buf.readUnsignedInt();

      millis = nanos / 1000000;

      // java timestamp, which is milliseconds
      timestamp = (seconds * 1000L) + millis;
      packetId = UUIDs.startOfJavaTimestamp(timestamp);

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
      parentDecoder.doCheckpoint();
    }

    while (readIndex < count) {
      //ByteBuf flowBuf = buf.slice(V5_HEADER_SIZE + (readIndex * V5_FLOW_SIZE), V5_FLOW_SIZE);

      NetflowV5Message msg = new NetflowV5Message();
      msg.setSeconds(seconds);
      msg.setNanos(nanos);
      msg.setCount(count);

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

      msg.setPacketId(packetId.toString());
      msg.setSender((sender == null) ? "unknown" : sender.getAddress().toString());
      msg.setLength(packetLength);
      msg.setUptime(uptime);
      msg.setTimestamp(timestamp);
      msg.setFlowSequence(flowSequence);
      msg.setEngineId(engineId);
      msg.setEngineType(engineType);
      msg.setRawSampling(sampling);
      msg.setSamplingInterval(samplingInterval);
      msg.setSamplingMode(samplingMode);
      msg.setReaderId(readerId);

      // 16
      long packets = buf.readUnsignedInt();
      // 20
      long octets = buf.readUnsignedInt();
      // 24
      long first = buf.readUnsignedInt();
      msg.setRawFirst(first);
      if (first > 0) {
        msg.setFirst(timestamp - uptime + first);
      } else {
        msg.setFirst(0L);
      }

      // 28
      long last = buf.readUnsignedInt();
      msg.setRawLast(last);
      if (last > 0) {
        msg.setLast(timestamp - uptime + last);
      } else {
        msg.setLast(0L);
      }

      msg.setId(UUIDs.timeBased().toString());
      msg.setSrcAddr(srcaddr);
      msg.setDstAddr(dstaddr);
      msg.setNextHop(nexthop);
      msg.setSrcAddrString(NetflowCommonDecoder.ipV4ToString(srcaddr));
      msg.setDstAddrString(NetflowCommonDecoder.ipV4ToString(dstaddr));
      msg.setNexthopString(NetflowCommonDecoder.ipV4ToString(nexthop));
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
      buf.skipBytes(2);
      result.add(msg);
      readIndex++;
      parentDecoder.doCheckpoint();
    }

    // if we reached this point without any further Signal errors, we have finished consuming
    //checkpoint();
    LinkedList<NetflowV5Message> returnResults = new LinkedList<>(result);
    resetState();
    return returnResults;
  }

  @Override
  public void resetState() {
    readHeader = false;
    count = 0;
    uptime = 0;
    seconds = 0;
    nanos = 0;
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
