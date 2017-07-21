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
package com.streamsets.pipeline.lib.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.net.netflow.v5.NetflowV5Message;
import com.streamsets.pipeline.lib.udp.UDPMessage;
import com.streamsets.pipeline.lib.udp.UDPMessageSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import org.junit.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class UDPTestUtil {

  // Common Record Values
  private static final Field HOST = Field.create("ip-192-168-42-24.us-west-2.compute.internal");
  private static final Field INTERVAL_10S = Field.create(10737418240L);

  // Record Values
  public static final Map<String, Field> expectedRecord0 = new ImmutableMap.Builder<String, Field>()
    .put("host", HOST)
    .put("interval_hires", INTERVAL_10S)
    .put("time_hires", Field.create(1543270991079203521L))
    .put("plugin", Field.create("cpu"))
    .put("plugin_instance", Field.create("7"))
    .put("type", Field.create("cpu"))
    .put("type_instance", Field.create("nice"))
    .put("value", Field.create(0L))
    .build();

  public static final Map<String, Field> expectedRecord2 = new ImmutableMap.Builder<String, Field>()
    .put("host", HOST)
    .put("interval_hires", INTERVAL_10S)
    .put("time_hires", Field.create(1543270991079198152L))
    .put("plugin", Field.create("interface"))
    .put("plugin_instance", Field.create("utun0"))
    .put("type", Field.create("if_packets"))
    .put("type_instance", Field.create(""))
    .put("rx", Field.create(4282L))
    .put("tx", Field.create(4551L))
    .build();

  public static final Map<String, Field> expectedRecordNoInterval0 = new ImmutableMap.Builder<String, Field>()
    .put("host", HOST)
    .put("time_hires", Field.create(1543270991079203521L))
    .put("plugin", Field.create("cpu"))
    .put("plugin_instance", Field.create("7"))
    .put("type", Field.create("cpu"))
    .put("type_instance", Field.create("nice"))
    .put("value", Field.create(0L))
    .build();

  public static final Map<String, Field> expectedRecordNoInterval2 = new ImmutableMap.Builder<String, Field>()
    .put("host", HOST)
    .put("time_hires", Field.create(1543270991079198152L))
    .put("plugin", Field.create("interface"))
    .put("plugin_instance", Field.create("utun0"))
    .put("type", Field.create("if_packets"))
    .put("type_instance", Field.create(""))
    .put("rx", Field.create(4282L))
    .put("tx", Field.create(4551L))
    .build();

  // Common record values for encrypted test set
  private static final Field ENC_HOST = Field.create("ip-192-168-42-238.us-west-2.compute.internal");

  public static final Map<String, Field> encryptedRecord14 = new ImmutableMap.Builder<String, Field>()
    .put("host", ENC_HOST)
    .put("interval_hires", INTERVAL_10S)
    .put("time_hires", Field.create(1543510262623895761L))
    .put("plugin", Field.create("interface"))
    .put("plugin_instance", Field.create("en8"))
    .put("type", Field.create("if_octets"))
    .put("tx", Field.create(216413106L))
    .put("rx", Field.create(1324428131L))
    .build();

  public static final Map<String, Field> signedRecord15 = new ImmutableMap.Builder<String, Field>()
    .put("host", ENC_HOST)
    .put("interval_hires", INTERVAL_10S)
    .put("time_hires", Field.create(1543518938371503765L))
    .put("plugin", Field.create("interface"))
    .put("plugin_instance", Field.create("en0"))
    .put("type", Field.create("if_packets"))
    .put("type_instance", Field.create(""))
    .put("tx", Field.create(836136L))
    .put("rx", Field.create(1204494L))
    .build();

  public static void verifyCollectdRecord(Map<String, Field> expected, Record record) {
    Map<String, Field> actual = record.get("/").getValueAsMap();
    Assert.assertEquals(expected.size(), actual.size());
    Set<Map.Entry<String, Field>> difference = Sets.difference(expected.entrySet(), actual.entrySet());
    Assert.assertEquals(true, difference.isEmpty());
  }


  public static byte[] getUDPData(int type, byte[] data) throws IOException {
    InetSocketAddress recipient = new InetSocketAddress("127.0.0.1", 2000);
    InetSocketAddress sender = new InetSocketAddress("127.0.0.1", 3000);
    ByteBuf buffer = Unpooled.wrappedBuffer(data);
    DatagramPacket datagram = new DatagramPacket(buffer, recipient, sender);
    UDPMessage message = new UDPMessage(type, 1, datagram);
    UDPMessageSerializer serializer = new UDPMessageSerializer();
    return serializer.serialize(message);
  }

  /**
   * <p>This method is duplicated (copy-pasted) in NetflowTestUtil (under net-commonlib).  Why?</p>
   *
   * <p><b>Short explanation:</b> least bad of several bad options</p>
   *
   * <p><b>Full explanation:</b> This code needs to live in a test part of some module.  It is used by a Kafka test
   * (in protolib) that is copied into many different Kafka stagelibs.  So adding a new test-jar dependency to each
   * of those projects would be a large change, since test scope dependencies are not transitive (and therefore the
   * existing test-jar dependency on commonlib would not bring it in).  commonlib cannot depend on net-commonlib,
   * with net-commonlib also having a circular dependency back to commonlib.</p>
   *
   * @param records
   */
  public static void assertRecordsForTenPackets(List<Record> records) {
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
    Assert.assertEquals(10, records.size());
    assertNetflowRecord(records.get(0), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 9231, "247.193.164.155", "227.213.154.241",
        17, "2015-05-23T04:32:55.0059", "2015-05-23T04:32:55.0059", 504, 1, 0, 89);
    assertNetflowRecord(records.get(1), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 64042, "247.193.164.155", "227.213.154.241",
        17, "2015-05-23T04:32:55.0059", "2015-05-23T04:32:55.0059", 504, 1, 0, 89);
    assertNetflowRecord(records.get(2), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 18784, "247.193.164.155", "227.213.154.241",
        17, "2015-05-23T04:32:55.0059", "2015-05-23T04:32:55.0059", 504, 1, 0, 89);
    assertNetflowRecord(records.get(3), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 43998, "249.229.186.21", "227.213.154.241",
        17, "2015-05-23T04:32:55.0061", "2015-05-23T04:32:55.0061", 504, 1, 0, 89);
    assertNetflowRecord(records.get(4), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 8790, "127.227.189.185", "227.213.154.241",
        17, "2015-05-23T04:32:55.0061", "2015-05-23T04:32:55.0061", 504, 1, 0, 89);
    assertNetflowRecord(records.get(5), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 38811, "127.227.189.185", "227.213.154.241",
        17, "2015-05-23T04:32:55.0061", "2015-05-23T04:32:55.0061", 504, 1, 0, 89);
    assertNetflowRecord(records.get(6), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 48001, "127.227.189.185", "227.213.154.241",
        17, "2015-05-23T04:32:55.0061", "2015-05-23T04:32:55.0061", 504, 1, 0, 89);
    assertNetflowRecord(records.get(7), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 57572, "249.229.186.21", "227.213.154.241",
        17, "2015-05-23T04:32:55.0061", "2015-05-23T04:32:55.0061", 504, 1, 0, 89);
    assertNetflowRecord(records.get(8), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 54356, "45.103.41.119", "227.213.154.241",
        17, "2015-05-23T04:32:55.0063", "2015-05-23T04:32:55.0063", 504, 1, 0, 696);
    assertNetflowRecord(records.get(9), 5, "2a9ac4fc-7c25-1000-8080-808080808080", 53, 5557, "121.75.53.47", "227.213.154.241",
        17, "2015-05-23T04:32:55.0064", "2015-05-23T04:32:55.0064", 504, 1, 0, 504);
  }

  private static void assertNetflowRecord(
      Record record,
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
    Map<String, Field> map = record.get().getValueAsMap();
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
}
