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
import org.junit.Assert;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public abstract class NetflowTestUtil {
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
    assertNetflowRecord(records.get(0), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 9231, "247.193.164.155", "227.213.154.241",
      17, "2015-04-12T21:32:19.0577", "2015-04-12T21:32:19.0577", 504, 1, 0, 89);
    assertNetflowRecord(records.get(1), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 64042, "247.193.164.155", "227.213.154.241",
      17, "2015-04-12T21:32:19.0577", "2015-04-12T21:32:19.0577", 504, 1, 0, 89);
    assertNetflowRecord(records.get(2), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 18784, "247.193.164.155", "227.213.154.241",
      17, "2015-04-12T21:32:19.0577", "2015-04-12T21:32:19.0577", 504, 1, 0, 89);
    assertNetflowRecord(records.get(3), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 43998, "249.229.186.21", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertNetflowRecord(records.get(4), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 8790, "127.227.189.185", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertNetflowRecord(records.get(5), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 38811, "127.227.189.185", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertNetflowRecord(records.get(6), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 48001, "127.227.189.185", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertNetflowRecord(records.get(7), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 57572, "249.229.186.21", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertNetflowRecord(records.get(8), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 54356, "45.103.41.119", "227.213.154.241",
      17, "2015-04-12T21:32:19.0573", "2015-04-12T21:32:19.0573", 504, 1, 0, 696);
    assertNetflowRecord(records.get(9), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 5557, "121.75.53.47", "227.213.154.241",
      17, "2015-04-12T21:32:19.0572", "2015-04-12T21:32:19.0572", 504, 1, 0, 504);
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
    SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSSS");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    Map<String, Field> map = record.get().getValueAsMap();
    Assert.assertEquals(version, map.get(NetflowMessage.VERSION).getValueAsInteger());
    Assert.assertEquals(packetId, map.get(NetflowMessage.PACKETID).getValueAsString());
    Assert.assertEquals(srcport, map.get(NetflowMessage.SRCPORT).getValueAsInteger());
    Assert.assertEquals(dstport, map.get(NetflowMessage.DSTPORT).getValueAsInteger());
    Assert.assertEquals(srcaddr, map.get(NetflowMessage.SRCADDR_S).getValueAsString());
    Assert.assertEquals(dstaddr, map.get(NetflowMessage.DSTADDR_S).getValueAsString());
    Assert.assertEquals(proto, map.get(NetflowMessage.PROTO).getValueAsInteger());
    Assert.assertEquals(length, map.get(NetflowMessage.LENGTH).getValueAsInteger());
    Assert.assertEquals(packets, map.get(NetflowMessage.PACKETS).getValueAsInteger());
    Assert.assertEquals(seq, map.get(NetflowMessage.FLOWSEQ).getValueAsInteger());
    Assert.assertEquals(octets, map.get(NetflowMessage.DOCTECTS).getValueAsInteger());
    Assert.assertEquals(first, dateFormat.format(new Date(map.get(NetflowMessage.FIRST).getValueAsLong())));
    Assert.assertEquals(last, dateFormat.format(new Date(map.get(NetflowMessage.LAST).getValueAsLong())));
  }
}
