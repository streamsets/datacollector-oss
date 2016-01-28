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
package com.streamsets.pipeline.lib.parser.netflow;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.AbstractParser;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by reading:
 * <a href="https://github.com/brockn/netflow">ASF licensed scala based netflow</a>,
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9 spec</a>,
 * and
 * <a href="http://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html#wp1003394">v1 and v5 spec</a>
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9</a>.
 */

public class NetflowParser extends AbstractParser {
  private static final int V9_HEADER_SIZE = 20;
  private static final int V5_HEADER_SIZE = 24;
  private static final int V5_FLOW_SIZE = 48;

  public static final String VERSION = "version";
  public static final String PACKETID = "packetid";

  public static final String ID = "id";
  public static final String SENDER = "sender";
  public static final String LENGTH = "length";
  public static final String UPTIME = "uptime";
  public static final String TIMESTAMP = "timestamp";
  public static final String SRCPORT = "srcport";
  public static final String DSTPORT = "dstport";
  public static final String SRCAS = "srcas";
  public static final String DSTAS = "dstas";
  public static final String PACKETS = "dPkts";
  public static final String DOCTECTS = "dOctets";
  public static final String PROTO = "proto";
  public static final String TOS = "tos";
  public static final String TCPFLAGS = "tcp_flags";
  public static final String FIRST = "first";
  public static final String LAST = "last";
  public static final String SRCADDR = "srcaddr";
  public static final String DSTADDR = "dstaddr";
  public static final String NEXTHOP = "nexthop";
  public static final String SRCADDR_S = "srcaddr_s";
  public static final String DSTADDR_S = "dstaddr_s";
  public static final String NEXTHOP_S = "nexthop_s";
  public static final String SNMPINPUT = "snmpinput";
  public static final String SNMPOUTPUT = "snmponput";
  public static final String SRCMASK = "src_mask";
  public static final String DSTMASK = "dst_mask";
  public static final String READERID = "readerId";

  public static final String FLOWSEQ = "flowseq";
  public static final String ENGINETYPE = "enginetype";
  public static final String ENGINEID = "engineid";
  public static final String SAMPLINGINT = "samplingint";
  public static final String SAMPLINGMODE = "samplingmode";

  private long recordId;

  public NetflowParser(Stage.Context context) {
    super(context);
    this.recordId = 0;
  }

  @Override
  public List<Record> parse(ByteBuf buf, InetSocketAddress recipient, InetSocketAddress sender)
    throws OnRecordErrorException {
    int packetLength = buf.readableBytes();
    if (packetLength < 4) {
      throw new OnRecordErrorException(Errors.NETFLOW_01,
        Utils.format("Packet must be at least 4 bytes, was: {}", packetLength));
    }
    String readerId = String.valueOf(recipient);
    int version = buf.getUnsignedShort(0); // 0-1
    switch (version) {
      case 5:
        return parseV5(version, packetLength, buf, readerId, sender);
      default:
        throw new OnRecordErrorException(Errors.NETFLOW_00, version);
    }
  }

  private List<Record> parseV5(int version, int packetLength, ByteBuf buf, String readerId,
                               InetSocketAddress sender)
    throws OnRecordErrorException {
    int count = buf.getUnsignedShort(2); // 2-3
    if (count <= 0) {
      throw new OnRecordErrorException(Errors.NETFLOW_01,Utils.format("Count is invalid: {}", count));
    } else if (packetLength < V5_HEADER_SIZE + count * V5_FLOW_SIZE) {
      String msg = Utils.format("Readable bytes {} too small for count {} (max {})",
        packetLength, count, (V5_HEADER_SIZE + count * V5_FLOW_SIZE));
      throw new OnRecordErrorException(Errors.NETFLOW_01, msg);
    }
    List<Record> result = new ArrayList<>(count);
    long uptime = buf.getUnsignedInt(4); // 4-7
    long seconds = buf.getUnsignedInt(8); // 8-11
    long millis = buf.getUnsignedInt(8) / 1000; // 12-15
    long timestamp = (seconds * 1000L) + millis; // java timestamp, which is milliseconds
    UUID packetId = UUIDs.startOfJavaTimestamp(timestamp);
    long flowSequence = buf.getUnsignedInt(16); // 16-19
    short engineType = buf.getUnsignedByte(20); // 20
    short engineId = buf.getUnsignedByte(21); // 21
    // the first 2 bits are the sampling mode, the remaining 14 the interval
    int sampling = buf.getUnsignedShort(22); // 22-23
    int samplingInterval = sampling & 0x3FFF;
    int samplingMode = sampling >> 14;
    Map<String, Field> headers = new HashMap<>();
    headers.put(VERSION, Field.create(version));
    headers.put(PACKETID, Field.create(packetId.toString()));
    headers.put(SENDER, Field.create((sender == null) ? "unknown" : sender.getAddress().toString()));
    headers.put(LENGTH, Field.create(packetLength));
    headers.put(UPTIME, Field.create(uptime));
    headers.put(TIMESTAMP, Field.create(timestamp));
    headers.put(FLOWSEQ, Field.create(flowSequence));
    headers.put(ENGINEID, Field.create(engineId));
    headers.put(ENGINETYPE, Field.create(engineType));
    headers.put(SAMPLINGINT, Field.create(samplingInterval));
    headers.put(SAMPLINGMODE, Field.create(samplingMode));
    headers.put(READERID, Field.create(readerId));
    for (int i = 0; i < count; i++) {
      ByteBuf flowBuf = buf.slice(V5_HEADER_SIZE + (i * V5_FLOW_SIZE), V5_FLOW_SIZE);
      Map<String, Field> fields = new HashMap<>();
      fields.putAll(headers);
      long pkts = flowBuf.getUnsignedInt(16);
      long bytes = flowBuf.getUnsignedInt(20);
      fields.put(ID, Field.create(UUIDs.timeBased().toString()));
      int srcaddr = (int)flowBuf.getUnsignedInt(0);
      int dstaddr = (int)flowBuf.getUnsignedInt(4);
      int nexthop = (int)flowBuf.getUnsignedInt(8);
      fields.put(SRCADDR, Field.create(srcaddr));
      fields.put(DSTADDR, Field.create(dstaddr));
      fields.put(NEXTHOP, Field.create(nexthop));
      fields.put(SRCADDR_S, Field.create(ipToString(srcaddr)));
      fields.put(DSTADDR_S, Field.create(ipToString(dstaddr)));
      fields.put(NEXTHOP_S, Field.create(ipToString(nexthop)));
      fields.put(SRCPORT, Field.create(flowBuf.getUnsignedShort(32)));
      fields.put(DSTPORT, Field.create(flowBuf.getUnsignedShort(34)));
      fields.put(SRCAS, Field.create(flowBuf.getUnsignedShort(40)));
      fields.put(DSTAS, Field.create(flowBuf.getUnsignedShort(42)));
      fields.put(PACKETS, Field.create(pkts));
      fields.put(DOCTECTS, Field.create(bytes));
      fields.put(PROTO, Field.create(flowBuf.getUnsignedByte(38)));
      fields.put(TOS, Field.create(flowBuf.getUnsignedByte(39)));
      fields.put(TCPFLAGS, Field.create(flowBuf.getUnsignedByte(37)));
      long first = flowBuf.getUnsignedInt(24);
      if (first > 0) {
        fields.put(FIRST, Field.create(timestamp - uptime - first));
      } else {
        fields.put(FIRST, Field.create(0L));
      }
      long last = flowBuf.getUnsignedInt(28);
      if (last > 0) {
        fields.put(LAST, Field.create(timestamp - uptime - last));
      } else {
        fields.put(LAST, Field.create(0L));
      }
      fields.put(SNMPINPUT, Field.create(flowBuf.getUnsignedShort(12)));
      fields.put(SNMPOUTPUT, Field.create(flowBuf.getUnsignedShort(14)));
      fields.put(SRCMASK, Field.create(flowBuf.getUnsignedByte(44)));
      fields.put(DSTMASK, Field.create(flowBuf.getUnsignedByte(45)));
      Record record = context.createRecord(readerId + "::" + recordId++);
      record.set(Field.create(fields));
      result.add(record);

    }
    return result;
  }
  private static String ipToString(int ip) {
    return String.format("%d.%d.%d.%d",
      (ip >> 24 & 0xff),
      (ip >> 16 & 0xff),
      (ip >> 8 & 0xff),
      (ip & 0xff));
  }
}
