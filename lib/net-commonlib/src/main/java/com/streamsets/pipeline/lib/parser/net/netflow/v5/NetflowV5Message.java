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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.net.netflow.BaseNetflowMessage;

import java.util.HashMap;
import java.util.Map;

public class NetflowV5Message extends BaseNetflowMessage {

  public static final String FIELD_VERSION = "version";
  public static final String FIELD_PACKETID = "packetid";
  public static final String FIELD_COUNT = "count";

  public static final String FIELD_SECONDS = "seconds";
  public static final String FIELD_NANOS = "nanos";

  public static final String FIELD_ID = "id";
  public static final String FIELD_SENDER = "sender";
  public static final String FIELD_LENGTH = "length";
  public static final String FIELD_UPTIME = "uptime";
  public static final String FIELD_TIMESTAMP = "timestamp";
  public static final String FIELD_SRCPORT = "srcport";
  public static final String FIELD_DSTPORT = "dstport";
  public static final String FIELD_SRCAS = "srcas";
  public static final String FIELD_DSTAS = "dstas";
  public static final String FIELD_PACKETS = "dPkts";
  public static final String FIELD_DOCTECTS = "dOctets";
  public static final String FIELD_PROTO = "proto";
  public static final String FIELD_TOS = "tos";
  public static final String FIELD_TCPFLAGS = "tcp_flags";
  public static final String FIELD_FIRST = "first";
  public static final String FIELD_RAW_FIRST = "raw_first";
  public static final String FIELD_LAST = "last";
  public static final String FIELD_RAW_LAST = "raw_last";
  public static final String FIELD_SRCADDR = "srcaddr";
  public static final String FIELD_DSTADDR = "dstaddr";
  public static final String FIELD_NEXTHOP = "nexthop";
  public static final String FIELD_SRCADDR_S = "srcaddr_s";
  public static final String FIELD_DSTADDR_S = "dstaddr_s";
  public static final String FIELD_NEXTHOP_S = "nexthop_s";
  public static final String FIELD_SNMPINPUT = "snmpinput";
  public static final String FIELD_SNMPOUTPUT = "snmponput";
  public static final String FIELD_SRCMASK = "src_mask";
  public static final String FIELD_DSTMASK = "dst_mask";
  public static final String FIELD_READERID = "readerId";

  public static final String FIELD_FLOWSEQ = "flowseq";
  public static final String FIELD_ENGINETYPE = "enginetype";
  public static final String FIELD_ENGINEID = "engineid";
  public static final String FIELD_RAW_SAMPLING = "raw_sampling";
  public static final String FIELD_SAMPLINGINT = "samplingint";
  public static final String FIELD_SAMPLINGMODE = "samplingmode";

  private String packetId;

  private int count;
  private String id;
  private String sender;
  private int length;
  private long uptime;
  private long timestamp;
  private int srcPort;
  private int dstPort;
  private int srcAs;
  private int dstAs;
  private long dPkts;
  private long dOctets;
  private short proto;
  private short tos;
  private short tcpFlags;
  private long first;
  private long last;
  private int srcAddr;
  private int dstAddr;
  private int nextHop;
  private String srcAddrString;
  private String dstAddrString;
  private String nexthopString;
  private int snmpInput;
  private int snmpOnput;
  private short srcMask;
  private short dstMask;
  private String readerId;

  private long flowSequence;
  private short engineType;
  private short engineId;

  private int rawSampling;
  private int samplingInterval;
  private int samplingMode;

  private long seconds;
  private long nanos;
  private long rawFirst;
  private long rawLast;

  @Override
  public int getNetflowVersion() {
    return 5;
  }

  public String getPacketId() {
    return packetId;
  }

  public void setPacketId(String packetId) {
    this.packetId = packetId;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getSender() {
    return sender;
  }

  public void setSender(String sender) {
    this.sender = sender;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public long getUptime() {
    return uptime;
  }

  public void setUptime(long uptime) {
    this.uptime = uptime;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getSrcPort() {
    return srcPort;
  }

  public void setSrcPort(int srcPort) {
    this.srcPort = srcPort;
  }

  public int getDstPort() {
    return dstPort;
  }

  public void setDstPort(int dstPort) {
    this.dstPort = dstPort;
  }

  public int getSrcAs() {
    return srcAs;
  }

  public void setSrcAs(int srcAs) {
    this.srcAs = srcAs;
  }

  public int getDstAs() {
    return dstAs;
  }

  public void setDstAs(int dstAs) {
    this.dstAs = dstAs;
  }

  public long getdPkts() {
    return dPkts;
  }

  public void setdPkts(long dPkts) {
    this.dPkts = dPkts;
  }

  public long getdOctets() {
    return dOctets;
  }

  public void setdOctets(long dOctets) {
    this.dOctets = dOctets;
  }

  public short getProto() {
    return proto;
  }

  public void setProto(short proto) {
    this.proto = proto;
  }

  public short getTos() {
    return tos;
  }

  public void setTos(short tos) {
    this.tos = tos;
  }

  public short getTcpFlags() {
    return tcpFlags;
  }

  public void setTcpFlags(short tcpFlags) {
    this.tcpFlags = tcpFlags;
  }

  public long getFirst() {
    return first;
  }

  public void setFirst(long first) {
    this.first = first;
  }

  public long getLast() {
    return last;
  }

  public void setLast(long last) {
    this.last = last;
  }

  public int getSrcAddr() {
    return srcAddr;
  }

  public void setSrcAddr(int srcAddr) {
    this.srcAddr = srcAddr;
  }

  public int getDstAddr() {
    return dstAddr;
  }

  public void setDstAddr(int dstAddr) {
    this.dstAddr = dstAddr;
  }

  public int getNextHop() {
    return nextHop;
  }

  public void setNextHop(int nextHop) {
    this.nextHop = nextHop;
  }

  public String getSrcAddrString() {
    return srcAddrString;
  }

  public void setSrcAddrString(String srcAddrString) {
    this.srcAddrString = srcAddrString;
  }

  public String getDstAddrString() {
    return dstAddrString;
  }

  public void setDstAddrString(String dstAddrString) {
    this.dstAddrString = dstAddrString;
  }

  public String getNexthopString() {
    return nexthopString;
  }

  public void setNexthopString(String nexthopString) {
    this.nexthopString = nexthopString;
  }

  public int getSnmpInput() {
    return snmpInput;
  }

  public void setSnmpInput(int snmpInput) {
    this.snmpInput = snmpInput;
  }

  public int getSnmpOnput() {
    return snmpOnput;
  }

  public void setSnmpOnput(int snmpOnput) {
    this.snmpOnput = snmpOnput;
  }

  public short getSrcMask() {
    return srcMask;
  }

  public void setSrcMask(short srcMask) {
    this.srcMask = srcMask;
  }

  public short getDstMask() {
    return dstMask;
  }

  public void setDstMask(short dstMask) {
    this.dstMask = dstMask;
  }

  public String getReaderId() {
    return readerId;
  }

  public void setReaderId(String readerId) {
    this.readerId = readerId;
  }

  public long getFlowSequence() {
    return flowSequence;
  }

  public void setFlowSequence(long flowSequence) {
    this.flowSequence = flowSequence;
  }

  public short getEngineType() {
    return engineType;
  }

  public void setEngineType(short engineType) {
    this.engineType = engineType;
  }

  public short getEngineId() {
    return engineId;
  }

  public void setEngineId(short engineId) {
    this.engineId = engineId;
  }

  public int getRawSampling() {
    return rawSampling;
  }

  public void setRawSampling(int rawSampling) {
    this.rawSampling = rawSampling;
  }

  public int getSamplingInterval() {
    return samplingInterval;
  }

  public void setSamplingInterval(int samplingInterval) {
    this.samplingInterval = samplingInterval;
  }

  public int getSamplingMode() {
    return samplingMode;
  }

  public void setSamplingMode(int samplingMode) {
    this.samplingMode = samplingMode;
  }

  public long getSeconds() {
    return seconds;
  }

  public void setSeconds(long seconds) {
    this.seconds = seconds;
  }

  public long getNanos() {
    return nanos;
  }

  public void setNanos(long nanos) {
    this.nanos = nanos;
  }

  public long getRawFirst() {
    return rawFirst;
  }

  public void setRawFirst(long rawFirst) {
    this.rawFirst = rawFirst;
  }

  public long getRawLast() {
    return rawLast;
  }

  public void setRawLast(long rawLast) {
    this.rawLast = rawLast;
  }

  @Override
  public void populateRecord(Record record) {
    Map<String, Field> fields = new HashMap<>();
    fields.put(FIELD_VERSION, Field.create(getNetflowVersion()));
    fields.put(FIELD_PACKETID, Field.create(getPacketId()));
    fields.put(FIELD_SENDER, Field.create(getSender()));
    fields.put(FIELD_LENGTH, Field.create(getLength()));
    fields.put(FIELD_UPTIME, Field.create(getUptime()));
    fields.put(FIELD_SECONDS, Field.create(getSeconds()));
    fields.put(FIELD_NANOS, Field.create(getNanos()));
    fields.put(FIELD_TIMESTAMP, Field.create(getTimestamp()));
    fields.put(FIELD_FLOWSEQ, Field.create(getFlowSequence()));
    fields.put(FIELD_ENGINEID, Field.create(getEngineId()));
    fields.put(FIELD_ENGINETYPE, Field.create(getEngineType()));
    fields.put(FIELD_RAW_SAMPLING, Field.create(getRawSampling()));
    fields.put(FIELD_SAMPLINGINT, Field.create(getSamplingInterval()));
    fields.put(FIELD_SAMPLINGMODE, Field.create(getSamplingMode()));
    fields.put(FIELD_READERID, Field.create(getReaderId()));

    fields.put(FIELD_COUNT, Field.create(getCount()));
    fields.put(FIELD_ID, Field.create(getId()));
    fields.put(FIELD_SRCADDR, Field.create(getSrcAddr()));
    fields.put(FIELD_DSTADDR, Field.create(getDstAddr()));
    fields.put(FIELD_NEXTHOP, Field.create(getNextHop()));
    fields.put(FIELD_SRCADDR_S, Field.create(getSrcAddrString()));
    fields.put(FIELD_DSTADDR_S, Field.create(getDstAddrString()));
    fields.put(FIELD_NEXTHOP_S, Field.create(getNexthopString()));
    fields.put(FIELD_SRCPORT, Field.create(getSrcPort()));
    fields.put(FIELD_DSTPORT, Field.create(getDstPort()));
    fields.put(FIELD_SRCAS, Field.create(getSrcAs()));
    fields.put(FIELD_DSTAS, Field.create(getDstAs()));
    fields.put(FIELD_PACKETS, Field.create(getdPkts()));
    fields.put(FIELD_DOCTECTS, Field.create(getdOctets()));
    fields.put(FIELD_PROTO, Field.create(getProto()));
    fields.put(FIELD_TOS, Field.create(getTos()));
    fields.put(FIELD_TCPFLAGS, Field.create(getTcpFlags()));
    fields.put(FIELD_FIRST, Field.create(getFirst()));
    fields.put(FIELD_RAW_FIRST, Field.create(getRawFirst()));
    fields.put(FIELD_LAST, Field.create(getLast()));
    fields.put(FIELD_RAW_LAST, Field.create(getRawLast()));
    fields.put(FIELD_SNMPINPUT, Field.create(getSnmpInput()));
    fields.put(FIELD_SNMPOUTPUT, Field.create(getSnmpOnput()));
    fields.put(FIELD_SRCMASK, Field.create(getSrcMask()));
    fields.put(FIELD_DSTMASK, Field.create(getDstMask()));

    record.set(Field.create(fields));
  }
}
