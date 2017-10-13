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

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.net.netflow.v5.NetflowV5Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public abstract class NetflowTestUtil {
  public static void assertRecordsForTenPackets(List<Record> records) {
    assertRecordsForTenPackets(records, null, 0, 10);
  }

  public static void assertRecordsForTenPackets(List<Record> records, int startIndex, int numRecords) {
    assertRecordsForTenPackets(records, null, startIndex, numRecords);
  }

  public static void assertRecordsForTenPackets(List<Record> records, String fieldPath) {
    assertRecordsForTenPackets(records, fieldPath, 0, 10);
  }

  public static void assertRecordsForTenPackets(List<Record> records, String fieldPath, int startIndex, int numRecords) {
    //  seq:1 [227.213.154.241]:9231 <> [247.193.164.155]:53 proto:17 octets>:0 packets>:0 octets<:89 packets<:1 start:2013-08-14T22:56:40.140733193388244 finish:2013-08-14T22:56:40.140733193388244 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe073801a70)
//  seq:2 [227.213.154.241]:64042 <> [247.193.164.155]:53 proto:17 octets>:0 packets>:0 octets<:89 packets<:1 start:2013-08-14T22:56:40.140733193388244 finish:2013-08-14T22:56:40.140733193388244 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe0738019e0)
//  seq:3 [227.213.154.241]:18784 <> [247.193.164.155]:53 proto:17 octets>:0 packets>:0 octets<:89 packets<:1 start:2013-08-14T22:56:40.140733193388244 finish:2013-08-14T22:56:40.140733193388244 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe073801950)
//  seq:4 [227.213.154.241]:43998 <> [249.229.186.21]:53 proto:17 octets>:0 packets>:0 octets<:89 packets<:1 start:2013-08-14T22:56:40.140733193388246 finish:2013-08-14T22:56:40.140733193388246 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe0738018c0)
//  seq:5 [127.227.189.185]:53 <> [227.213.154.241]:8790 proto:17 octets>:89 packets>:1 octets<:0 packets<:0 start:2013-08-14T22:56:40.140733193388246 finish:2013-08-14T22:56:40.140733193388246 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe073801830)
//  seq:6 [127.227.189.185]:53 <> [227.213.154.241]:38811 proto:17 octets>:89 packets>:1 octets<:0 packets<:0 start:2013-08-14T22:56:40.140733193388246 finish:2013-08-14T22:56:40.140733193388246 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe0738017a0)
//  seq:7 [127.227.189.185]:53 <> [227.213.154.241]:48001 proto:17 octets>:89 packets>:1 octets<:0 packets<:0 start:2013-08-14T22:56:40.140733193388246 finish:2013-08-14T22:56:40.140733193388246 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe073801710)
//  seq:8 [227.213.154.241]:57572 <> [249.229.186.21]:53 proto:17 octets>:0 packets>:0 octets<:89 packets<:1 start:2013-08-14T22:56:40.140733193388246 finish:2013-08-14T22:56:40.140733193388246 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe073801680)
//  seq:9 [45.103.41.119]:53 <> [227.213.154.241]:54356 proto:17 octets>:696 packets>:1 octets<:0 packets<:0 start:2013-08-14T22:56:40.140733193388248 finish:2013-08-14T22:56:40.140733193388248 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe0738015f0)
//  seq:10 [121.75.53.47]:53 <> [227.213.154.241]:5557 proto:17 octets>:504 packets>:1 octets<:0 packets<:0 start:2013-08-14T22:56:40.140733193388249 finish:2013-08-14T22:56:40.140733193388249 tcp>:00 tcp<:00 flowlabel>:00000000 flowlabel<:00000000  (0x7fe073801560)
    Assert.assertEquals(numRecords, records.size());

    for (int i = 0; i < records.size(); i++) {
      final int recordIndex = startIndex + i;
      final Record record = records.get(i);
      switch (recordIndex % 10) {
        case 0:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              9231,
              "247.193.164.155",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0059",
              "2015-05-23T04:32:55.0059",
              504,
              1,
              0,
              89
          );
          break;
        case 1:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              64042,
              "247.193.164.155",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0059",
              "2015-05-23T04:32:55.0059",
              504,
              1,
              0,
              89
          );
          break;
        case 2:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              18784,
              "247.193.164.155",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0059",
              "2015-05-23T04:32:55.0059",
              504,
              1,
              0,
              89
          );
          break;
        case 3:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              43998,
              "249.229.186.21",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0061",
              "2015-05-23T04:32:55.0061",
              504,
              1,
              0,
              89
          );
          break;
        case 4:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              8790,
              "127.227.189.185",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0061",
              "2015-05-23T04:32:55.0061",
              504,
              1,
              0,
              89
          );
          break;
        case 5:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              38811,
              "127.227.189.185",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0061",
              "2015-05-23T04:32:55.0061",
              504,
              1,
              0,
              89
          );
          break;
        case 6:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              48001,
              "127.227.189.185",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0061",
              "2015-05-23T04:32:55.0061",
              504,
              1,
              0,
              89
          );
          break;
        case 7:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              57572,
              "249.229.186.21",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0061",
              "2015-05-23T04:32:55.0061",
              504,
              1,
              0,
              89
          );
          break;
        case 8:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              54356,
              "45.103.41.119",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0063",
              "2015-05-23T04:32:55.0063",
              504,
              1,
              0,
              696
          );
          break;
        case 9:
          assertNetflowRecord(
              record,
              fieldPath,
              5,
              "2a9ac4fc-7c25-1000-8080-808080808080",
              53,
              5557,
              "121.75.53.47",
              "227.213.154.241",
              17,
              "2015-05-23T04:32:55.0064",
              "2015-05-23T04:32:55.0064",
              504,
              1,
              0,
              504
          );
          break;
        default:
          throw new IllegalStateException("any number mod 10 should have 10 possibilities");
      }
    }
  }

  private static void assertNetflowRecord(
    Record record,
    String fieldPath,
    int version,
    String packetId,
    int srcport,
    int dstport,
    String srcaddr,
    String dstaddr,
    int proto,
    String first,
    String last,
    int length,
    int packets,
    int seq,
    int octets
  ) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    Field valueField = Strings.isNullOrEmpty(fieldPath) ? record.get() : record.get(fieldPath);
    Map<String, Field> map = valueField.getValueAsMap();
    Assert.assertEquals(version, map.get(NetflowV5Message.FIELD_VERSION).getValueAsInteger());
    Assert.assertEquals(packetId, map.get(NetflowV5Message.FIELD_PACKETID).getValueAsString());
    Assert.assertEquals(srcport, map.get(NetflowV5Message.FIELD_SRCPORT).getValueAsInteger());
    Assert.assertEquals(dstport, map.get(NetflowV5Message.FIELD_DSTPORT).getValueAsInteger());
    Assert.assertEquals(srcaddr, map.get(NetflowV5Message.FIELD_SRCADDR_S).getValueAsString());
    Assert.assertEquals(dstaddr, map.get(NetflowV5Message.FIELD_DSTADDR_S).getValueAsString());
    Assert.assertEquals(proto, map.get(NetflowV5Message.FIELD_PROTO).getValueAsInteger());
    Assert.assertEquals(length, map.get(NetflowV5Message.FIELD_LENGTH).getValueAsInteger());
    Assert.assertEquals(packets, map.get(NetflowV5Message.FIELD_PACKETS).getValueAsInteger());
    Assert.assertEquals(seq, map.get(NetflowV5Message.FIELD_FLOWSEQ).getValueAsInteger());
    Assert.assertEquals(octets, map.get(NetflowV5Message.FIELD_DOCTECTS).getValueAsInteger());
    final long uptime = map.get(NetflowV5Message.FIELD_UPTIME).getValueAsLong();
    final long timestamp = map.get(NetflowV5Message.FIELD_TIMESTAMP).getValueAsLong();
    final long calculatedFirst = map.get(NetflowV5Message.FIELD_FIRST).getValueAsLong();
    final long calculatedLast = map.get(NetflowV5Message.FIELD_LAST).getValueAsLong();
    final long rawFirst = map.get(NetflowV5Message.FIELD_RAW_FIRST).getValueAsLong();
    final long rawLast = map.get(NetflowV5Message.FIELD_RAW_LAST).getValueAsLong();
    Assert.assertEquals(timestamp - uptime + rawFirst, calculatedFirst);
    Assert.assertEquals(timestamp - uptime + rawLast, calculatedLast);
    Assert.assertEquals(first, dateFormat.format(new Date(calculatedFirst)));
    Assert.assertEquals(last, dateFormat.format(new Date(calculatedLast)));
  }

  public static void writeV5NetflowHeader(
      EmbeddedChannel channel,
      int count,
      long uptime,
      long seconds,
      long nanos,
      long flowSequence,
      int engineType,
      int engineId,
      int sampling
  ) {
    final int version = 5;

    final ByteBuf buf = Unpooled.buffer();
    buf.writeShort(version);
    buf.writeShort(count);
    buf.writeInt((int)uptime);
    buf.writeInt((int)seconds);
    buf.writeInt((int)nanos);
    buf.writeInt((int)flowSequence);
    buf.writeByte(engineType);
    buf.writeByte(engineId);
    buf.writeShort(sampling);

    channel.writeInbound(buf);
  }

  public static void writeV5NetflowFlowRecord(
      EmbeddedChannel channel,
      long srcAddr,
      long destAddr,
      long nextHop,
      int snmpInput,
      int snmpOutput,
      long packets,
      long octets,
      long first,
      long last,
      int srcPort,
      int destPort,
      int tcpFlags,
      int proto,
      int tos,
      int srcAs,
      int destAs,
      int srcMask,
      int destMask

  ) {

    final ByteBuf buf = Unpooled.buffer();
    buf.writeInt((int) srcAddr);
    buf.writeInt((int) destAddr);
    buf.writeInt((int) nextHop);
    buf.writeShort(snmpInput);
    buf.writeShort(snmpOutput);
    buf.writeInt((int)packets);
    buf.writeInt((int)octets);
    buf.writeInt((int)first);
    buf.writeInt((int)last);
    buf.writeShort(srcPort);
    buf.writeShort(destPort);
    // one empty pad byte
    buf.writeByte(0);
    buf.writeByte(tcpFlags);
    buf.writeByte(proto);
    buf.writeByte(tos);
    buf.writeShort(srcAs);
    buf.writeShort(destAs);
    buf.writeByte(srcMask);
    buf.writeByte(destMask);
    // two empty pad bytes
    buf.writeByte(0);
    buf.writeByte(0);
    channel.writeInbound(buf);
  }
}
