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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.net.MessageToRecord;

import java.util.HashMap;
import java.util.Map;

public class NetflowMessage implements MessageToRecord {

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

  private int version;
  private String packetId;

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
  private int samplingInterval;
  private int samplingMode;

  public NetflowMessage() {
    int k =14;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public String getPacketId() {
    return packetId;
  }

  public void setPacketId(String packetId) {
    this.packetId = packetId;
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

  @Override
  public void populateRecord(Record record) {
    Map<String, Field> fields = new HashMap<>();
    fields.put(VERSION, Field.create(getVersion()));
    fields.put(PACKETID, Field.create(getPacketId()));
    fields.put(SENDER, Field.create(getSender()));
    fields.put(LENGTH, Field.create(getLength()));
    fields.put(UPTIME, Field.create(getUptime()));
    fields.put(TIMESTAMP, Field.create(getTimestamp()));
    fields.put(FLOWSEQ, Field.create(getFlowSequence()));
    fields.put(ENGINEID, Field.create(getEngineId()));
    fields.put(ENGINETYPE, Field.create(getEngineType()));
    fields.put(SAMPLINGINT, Field.create(getSamplingInterval()));
    fields.put(SAMPLINGMODE, Field.create(getSamplingMode()));
    fields.put(READERID, Field.create(getReaderId()));

    fields.put(ID, Field.create(getId()));
    fields.put(SRCADDR, Field.create(getSrcAddr()));
    fields.put(DSTADDR, Field.create(getDstAddr()));
    fields.put(NEXTHOP, Field.create(getNextHop()));
    fields.put(SRCADDR_S, Field.create(getSrcAddrString()));
    fields.put(DSTADDR_S, Field.create(getDstAddrString()));
    fields.put(NEXTHOP_S, Field.create(getNexthopString()));
    fields.put(SRCPORT, Field.create(getSrcPort()));
    fields.put(DSTPORT, Field.create(getDstPort()));
    fields.put(SRCAS, Field.create(getSrcAs()));
    fields.put(DSTAS, Field.create(getDstAs()));
    fields.put(PACKETS, Field.create(getdPkts()));
    fields.put(DOCTECTS, Field.create(getdOctets()));
    fields.put(PROTO, Field.create(getProto()));
    fields.put(TOS, Field.create(getTos()));
    fields.put(TCPFLAGS, Field.create(getTcpFlags()));
    fields.put(FIRST, Field.create(getFirst()));
    fields.put(LAST, Field.create(getLast()));
    fields.put(SNMPINPUT, Field.create(getSnmpInput()));
    fields.put(SNMPOUTPUT, Field.create(getSnmpOnput()));
    fields.put(SRCMASK, Field.create(getSrcMask()));
    fields.put(DSTMASK, Field.create(getDstMask()));

    record.set(Field.create(fields));
  }
}
