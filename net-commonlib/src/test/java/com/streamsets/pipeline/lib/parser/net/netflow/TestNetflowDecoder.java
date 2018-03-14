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
package com.streamsets.pipeline.lib.parser.net.netflow;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.net.NetTestUtils;
import com.streamsets.pipeline.lib.parser.net.netflow.v5.NetflowV5Message;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.FlowKind;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.NetflowV9Field;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.NetflowV9FieldType;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.NetflowV9Message;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.testing.RandomTestUtils;
import com.streamsets.testing.ValueAccessor;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.lang3.RandomUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.streamsets.testing.Matchers.fieldWithValue;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasKey;
import static com.streamsets.testing.Matchers.mapFieldWithEntry;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class TestNetflowDecoder {
  private static final int PROTOCOL_UDP = 17;
  private static final int PROTOCOL_TCP = 6;

  @NotNull
  private NetflowCommonDecoder makeNetflowDecoder() {
    return new NetflowCommonDecoder(
        OutputValuesMode.RAW_AND_INTERPRETED,
        -1,
        -1
    );
  }

  @Test
  public void testSinglePacket() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(makeNetflowDecoder());

    byte[] bytes = get10V5MessagesBytes();
    ch.writeInbound(Unpooled.wrappedBuffer(bytes));

    List<Record> records = collect10NetflowV5MessagesFromChannel(ch, bytes.length);

    ch.finish();

    NetflowTestUtil.assertRecordsForTenPackets(records);
  }

  @Test
  public void testMultiplePackets() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(makeNetflowDecoder());

    byte[] bytes = get10V5MessagesBytes();

    long bytesWritten = 0;
    List<List<Byte>> slices = NetTestUtils.getRandomByteSlices(bytes);
    for (int s = 0; s<slices.size(); s++) {
      List<Byte> slice = slices.get(s);
      byte[] sliceBytes = Bytes.toArray(slice);
      ch.writeInbound(Unpooled.wrappedBuffer(sliceBytes));
      bytesWritten += sliceBytes.length;
    }

    assertThat(bytesWritten, equalTo((long)bytes.length));

    List<Record> records = collect10NetflowV5MessagesFromChannel(ch, bytes.length);

    ch.finish();

    NetflowTestUtil.assertRecordsForTenPackets(records);
  }

  private byte[] getV9MessagesBytes7Flows() throws IOException {
    return getMessagesBytes("netflow-v9-packet-7_flows.bin");
  }

  private byte[] get10V5MessagesBytes() throws IOException {
    return getMessagesBytes("netflow-v5-file-1");
  }

  private byte[] getMessagesBytes(String fileName) throws IOException {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(
        "com/streamsets/pipeline/lib/parser/net/netflow/" + fileName
    );
    return ByteStreams.toByteArray(is);
  }

  @NotNull
  private List<Record> collect10NetflowV5MessagesFromChannel(EmbeddedChannel ch, int packetLength) {
    List<Record> records = new LinkedList<>();
    for (int i=0; i<10; i++) {
      Object object = ch.readInbound();
      assertNotNull(object);
      assertThat(object, is(instanceOf(NetflowV5Message.class)));
      NetflowV5Message msg = (NetflowV5Message) object;
      // fix packet length for test; it passes in MAX_LENGTH by default
      msg.setLength(packetLength);
      Record record = RecordCreator.create();
      msg.populateRecord(record);
      records.add(record);
    }
    return records;
  }

  @NotNull
  private List<Record> collectNetflowV9MessagesFromChannel(
      EmbeddedChannel ch,
      int numMessages,
      List<NetflowV9Message> messages
  ) {
    List<Record> records = new LinkedList<>();
    for (int i = 0; i < numMessages; i++) {
      Object object = ch.readInbound();
      assertNotNull(object);
      assertThat(object, is(instanceOf(NetflowV9Message.class)));
      NetflowV9Message msg = (NetflowV9Message) object;
      messages.add(msg);
      Record record = RecordCreator.create();
      msg.populateRecord(record);
      records.add(record);
    }
    return records;
  }

  @Test
  public void testTimestamps() {

    EmbeddedChannel ch = new EmbeddedChannel(makeNetflowDecoder());

    final long uptime = RandomUtils.nextLong(0L, 1000000L);
    final long seconds = RandomUtils.nextLong(0L, 1500000000L);
    final long nanos = RandomUtils.nextLong(0L, 1000000000L-1L);
    NetflowTestUtil.writeV5NetflowHeader(ch, 1, uptime, seconds, nanos, 0L, 0, 0, 0);

    final long first = RandomUtils.nextLong(uptime + 1L, 2000000L);
    final long last = RandomUtils.nextLong(first + 1L, 3000000L);
    NetflowTestUtil.writeV5NetflowFlowRecord(ch, 0, 0, 0, 0, 0, 1, 1, first, last, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    Object obj = ch.readInbound();
    assertThat(obj, instanceOf(NetflowV5Message.class));
    NetflowV5Message msg = (NetflowV5Message) obj;

    assertThat(msg.getCount(), equalTo(1));
    assertThat(msg.getSeconds(), equalTo(seconds));
    assertThat(msg.getNanos(), equalTo(nanos));
    assertThat(msg.getUptime(), equalTo(uptime));

    final long expectedTimestamp = seconds * 1000 + (nanos/1000000);
    assertThat(msg.getTimestamp(), equalTo(expectedTimestamp));

    final long expectedFirst = expectedTimestamp - uptime + first;
    assertThat(msg.getFirst(), equalTo(expectedFirst));

    final long expectedLast = expectedTimestamp - uptime + last;
    assertThat(msg.getLast(), equalTo(expectedLast));
  }

  @Test
  public void netflowV9SevenFlows() throws Exception {
    // TODO: add tests that include options templates/data
    EmbeddedChannel ch = new EmbeddedChannel(makeNetflowDecoder());

    byte[] bytes = getV9MessagesBytes7Flows();

    final boolean randomlySlice = RandomTestUtils.getRandom().nextBoolean();
    writeBytesToChannel(ch, bytes, randomlySlice);

    final int expectedNumMsgs = 7;

    List<NetflowV9Message> messages = new ArrayList<>();
    List<Record> records = collectNetflowV9MessagesFromChannel(ch, expectedNumMsgs, messages);

    ch.finish();

    assertThat(messages, hasSize(expectedNumMsgs));
    assertThat(records, hasSize(expectedNumMsgs));

    for (int i = 0; i< expectedNumMsgs; i++) {
      NetflowV9Message message = messages.get(i);
      Record record = records.get(i);
      // same value for all flows
      int exportSourceId = 1;
      int size = 56;
      FlowKind kind = FlowKind.FLOWSET;

      // flow-specific values
      Integer tcpFlags = null;
      Long firstSwitched = null;
      Long lastSwitched = null;
      String srcIpV4Addr = null;
      String destIpV4Addr = null;
      Integer srcPort = null;
      Integer destPort = null;
      Integer protocol = null;
      BigDecimal inPackets = null;
      BigDecimal inBytes = null;
      switch (i) {
        case 0:
          tcpFlags = 0;
          firstSwitched = 86400042L;
          lastSwitched = 86940154L;
          srcIpV4Addr = "127.0.0.1";
          destIpV4Addr = "127.0.0.1";
          srcPort = 52767;
          destPort = 9995;
          protocol = PROTOCOL_UDP;
          inPackets = new BigDecimal(29);
          inBytes = new BigDecimal(34964);
          break;
        case 1:
          tcpFlags = 0;
          firstSwitched = 87028319L;
          lastSwitched = 87028320L;
          srcIpV4Addr = "172.17.0.4";
          destIpV4Addr = "192.168.65.1";
          srcPort = 34460;
          destPort = 53;
          protocol = PROTOCOL_UDP;
          inPackets = new BigDecimal(1);
          inBytes = new BigDecimal(75);
          break;
        case 2:
          tcpFlags = 0;
          firstSwitched = 87028319L;
          lastSwitched = 87028320L;
          srcIpV4Addr = "192.168.65.1";
          destIpV4Addr = "172.17.0.4";
          srcPort = 53;
          destPort = 34460;
          protocol = PROTOCOL_UDP;
          inPackets = new BigDecimal(1);
          inBytes = new BigDecimal(75);
          break;
        case 3:
          tcpFlags = 0;
          firstSwitched = 87028320L;
          lastSwitched = 87028333L;
          srcIpV4Addr = "172.17.0.4";
          destIpV4Addr = "192.168.65.1";
          srcPort = 48251;
          destPort = 53;
          protocol = PROTOCOL_UDP;
          inPackets = new BigDecimal(1);
          inBytes = new BigDecimal(64);
          break;
        case 4:
          tcpFlags = 0;
          firstSwitched = 87028320L;
          lastSwitched = 87028333L;
          srcIpV4Addr = "192.168.65.1";
          destIpV4Addr = "172.17.0.4";
          srcPort = 53;
          destPort = 48251;
          protocol = PROTOCOL_UDP;
          inPackets = new BigDecimal(1);
          inBytes = new BigDecimal(128);
          break;
        case 5:
          tcpFlags = 0x1b;
          firstSwitched = 87028333L;
          lastSwitched = 87059145L;
          srcIpV4Addr = "91.189.88.161";
          destIpV4Addr = "172.17.0.4";
          srcPort = 80;
          destPort = 51156;
          protocol = PROTOCOL_TCP;
          inPackets = new BigDecimal(25233);
          inBytes = new BigDecimal(36890729);
          break;
        case 6:
          tcpFlags = 0x1b;
          firstSwitched = 87028333L;
          lastSwitched = 87059145L;
          srcIpV4Addr = "172.17.0.4";
          destIpV4Addr = "91.189.88.161";
          srcPort = 51156;
          destPort = 80;
          protocol = PROTOCOL_TCP;
          inPackets = new BigDecimal(11214);
          inBytes = new BigDecimal(452409);
          break;
      }
      assertNetflowV9MessageAndRecord(
          kind,
          message,
          record,
          tcpFlags,
          exportSourceId,
          size,
          firstSwitched,
          lastSwitched,
          srcIpV4Addr,
          destIpV4Addr,
          srcPort,
          destPort,
          protocol,
          inPackets,
          inBytes
      );
    }
  }

  private void writeBytesToChannel(EmbeddedChannel ch, byte[] bytes, boolean randomlySlice) {
    if (randomlySlice) {
      long bytesWritten = 0;
      List<List<Byte>> slices = NetTestUtils.getRandomByteSlices(bytes);
      for (int s = 0; s<slices.size(); s++) {
        List<Byte> slice = slices.get(s);
        byte[] sliceBytes = Bytes.toArray(slice);
        ch.writeInbound(Unpooled.wrappedBuffer(sliceBytes));
        bytesWritten += sliceBytes.length;
      }

      assertThat(bytesWritten, equalTo((long)bytes.length));
    } else {
      ch.writeInbound(Unpooled.wrappedBuffer(bytes));
    }
  }

  @Test
  public void ciscoAsa() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(makeNetflowDecoder());

    boolean randomlySlice = RandomTestUtils.getRandom().nextBoolean();
    byte[] tplBytes = getMessagesBytes("ciscoasa/netflow9_test_cisco_asa_1_tpl.dat");
    writeBytesToChannel(ch, tplBytes, randomlySlice);

    byte[] msgBytes = getMessagesBytes("ciscoasa/netflow9_test_cisco_asa_1_data.dat");
    randomlySlice = RandomTestUtils.getRandom().nextBoolean();
    writeBytesToChannel(ch, msgBytes, randomlySlice);

    final int expectedNumMsgs = 9;

    List<NetflowV9Message> messages = new ArrayList<>();
    List<Record> records = collectNetflowV9MessagesFromChannel(ch, expectedNumMsgs, messages);

    ch.finish();

    assertThat(messages, hasSize(expectedNumMsgs));
    assertThat(records, hasSize(expectedNumMsgs));

  }
  public static void assertNetflowV9MessageAndRecord(
      FlowKind kind,
      NetflowV9Message message,
      Record record,
      Integer expectedTcpFlags,
      int expectedSourceId,
      int size,
      Long firstSwitched,
      Long lastSwitched,
      String srcIpV4Addr,
      String destIpV4Addr,
      Integer srcPort,
      Integer destPort,
      Integer protocol,
      BigDecimal inPackets,
      BigDecimal inBytes
  ) {
    // assertions on the SDC record

    assertThat(record.get(), mapFieldWithEntry(NetflowV9Message.FIELD_FLOW_KIND, kind.name()));

    final String interpretedValuesFieldPath = "/" + NetflowV9Message.FIELD_INTERPRETED_VALUES;
    assertTrue(record.has(interpretedValuesFieldPath));
    final Field interpretedValuesField = record.get(interpretedValuesFieldPath);
    assertThat(interpretedValuesField, notNullValue());

    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.TCP_FLAGS.name(), expectedTcpFlags));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.FIRST_SWITCHED.name(), firstSwitched));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.LAST_SWITCHED.name(), lastSwitched));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.IPV4_SRC_ADDR.name(), srcIpV4Addr));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.IPV4_DST_ADDR.name(), destIpV4Addr));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.L4_SRC_PORT.name(), srcPort));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.L4_DST_PORT.name(), destPort));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.PROTOCOL.name(), protocol));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.IN_PKTS.name(), inPackets));
    assertThat(interpretedValuesField, mapFieldWithEntry(NetflowV9FieldType.IN_BYTES.name(), inBytes));

    final String rawValuesFieldPath = "/" + NetflowV9Message.FIELD_RAW_VALUES;
    assertTrue(record.has(rawValuesFieldPath));
    final Field rawValuesField = record.get(rawValuesFieldPath);
    assertThat(rawValuesField, notNullValue());
    final String packetHeaderFieldPath = "/" + NetflowV9Message.FIELD_PACKET_HEADER;
    assertTrue(record.has(packetHeaderFieldPath));

    final Field packetHeaderField = record.get(packetHeaderFieldPath);
    assertThat(packetHeaderField, notNullValue());
    assertThat(packetHeaderField, mapFieldWithEntry(NetflowV9Message.FIELD_SOURCE_ID, expectedSourceId));

    // assertions on the Netflow 9 message object
    assertThat(message.getFlowKind(), equalTo(FlowKind.FLOWSET));

    final Map<NetflowV9FieldType, NetflowV9Field> interpretedValueFieldsByType = new HashMap<>();
    message.getFields().forEach(field -> interpretedValueFieldsByType.put(field.getFieldTemplate().getType(), field));

    assertIntField(interpretedValueFieldsByType, NetflowV9FieldType.TCP_FLAGS, expectedTcpFlags);
    assertLongField(interpretedValueFieldsByType, NetflowV9FieldType.FIRST_SWITCHED, firstSwitched);
    assertLongField(interpretedValueFieldsByType, NetflowV9FieldType.LAST_SWITCHED, lastSwitched);
    assertStringField(interpretedValueFieldsByType, NetflowV9FieldType.IPV4_SRC_ADDR, srcIpV4Addr);
    assertStringField(interpretedValueFieldsByType, NetflowV9FieldType.IPV4_DST_ADDR, destIpV4Addr);
    assertIntField(interpretedValueFieldsByType, NetflowV9FieldType.L4_SRC_PORT, srcPort);
    assertIntField(interpretedValueFieldsByType, NetflowV9FieldType.L4_DST_PORT, destPort);
    assertIntField(interpretedValueFieldsByType, NetflowV9FieldType.PROTOCOL, protocol);
    assertDecimalField(interpretedValueFieldsByType, NetflowV9FieldType.IN_PKTS, inPackets);
    assertDecimalField(interpretedValueFieldsByType, NetflowV9FieldType.IN_BYTES, inBytes);
  }

  private static void assertIntField(
      Map<NetflowV9FieldType, NetflowV9Field> fieldsByType,
      NetflowV9FieldType fieldType,
      Integer expectedValue
  ) {
    assertGenericField(
        fieldsByType,
        fieldType,
        Field::getValueAsInteger,
        expectedValue
    );
  }

  private static void assertLongField(
      Map<NetflowV9FieldType, NetflowV9Field> fieldsByType,
      NetflowV9FieldType fieldType,
      Long expectedValue
  ) {
    assertGenericField(
        fieldsByType,
        fieldType,
        Field::getValueAsLong,
        expectedValue
    );
  }

  private static void assertStringField(
      Map<NetflowV9FieldType, NetflowV9Field> fieldsByType,
      NetflowV9FieldType fieldType,
      String expectedValue
  ) {
    assertGenericField(
        fieldsByType,
        fieldType,
        Field::getValueAsString,
        expectedValue
    );
  }

  private static void assertDecimalField(
      Map<NetflowV9FieldType, NetflowV9Field> fieldsByType,
      NetflowV9FieldType fieldType,
      BigDecimal expectedValue
  ) {
    assertGenericField(
        fieldsByType,
        fieldType,
        Field::getValueAsDecimal,
        expectedValue
    );
  }

  private static <T> void assertGenericField(
      Map<NetflowV9FieldType, NetflowV9Field> fieldsByType,
      NetflowV9FieldType fieldType,
      ValueAccessor<T> actualValueAccessor,
      T expectedValue
  ) {
    if (expectedValue != null) {
      assertThat(fieldsByType, hasKey(fieldType));
      NetflowV9Field field = fieldsByType.get(fieldType);
      assertThat(field, notNullValue());
      final Field sdcField = field.getInterpretedValueField();
      assertThat(sdcField, notNullValue());
      assertThat(actualValueAccessor.getValue(sdcField), equalTo(expectedValue));
    } else {
      assertThat(fieldsByType, not(hasKey(fieldType)));
    }
  }

  @Test
  public void senderAndReceiver() throws IOException, OnRecordErrorException {
    final NetflowCommonDecoder decoder = makeNetflowDecoder();

    final byte[] bytes = getV9MessagesBytes7Flows();
    final List<BaseNetflowMessage> messages = new LinkedList<>();
    final InetSocketAddress senderAddr = InetSocketAddress.createUnresolved("hostA", 1234);
    final InetSocketAddress recipientAddr = InetSocketAddress.createUnresolved("hostB", 5678);
    decoder.decodeStandaloneBuffer(
        Unpooled.copiedBuffer(bytes),
        messages,
        senderAddr, recipientAddr
    );

    assertThat(messages, hasSize(7));
    final BaseNetflowMessage firstBaseMsg = messages.get(0);
    assertThat(firstBaseMsg, instanceOf(NetflowV9Message.class));
    final NetflowV9Message firstMsg = (NetflowV9Message) firstBaseMsg;
    assertThat(firstMsg.getSender(), notNullValue());
    assertThat(firstMsg.getRecipient(), notNullValue());
    assertThat(firstMsg.getSender().toString(), equalTo(senderAddr.toString()));
    assertThat(firstMsg.getRecipient().toString(), equalTo(recipientAddr.toString()));

    Record record = RecordCreator.create();
    firstMsg.populateRecord(record);

    assertThat(record.get("/" + NetflowV9Message.FIELD_SENDER), fieldWithValue(senderAddr.toString()));
    assertThat(record.get("/" + NetflowV9Message.FIELD_RECIPIENT), fieldWithValue(recipientAddr.toString()));
  }

}
