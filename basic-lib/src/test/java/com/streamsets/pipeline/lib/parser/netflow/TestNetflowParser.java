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
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;
import org.junit.Assert;
import java.io.File;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class TestNetflowParser {
  private static final File TEN_PACKETS = new File(System.getProperty("user.dir") +
    "/src/test/resources/netflow-v5-file-1");
  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidVersion() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(4);
    buf.writeShort(0);
    buf.writeShort(0);
    netflowParser.parse(buf, null, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidCountInvalidLength() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(4);
    buf.writeShort(5);
    buf.writeShort(1);
    netflowParser.parse(buf, null, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidCountZero() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(4);
    buf.writeShort(5);
    buf.writeShort(0);
    netflowParser.parse(buf, null, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidPacketTooShort1() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(0);
    netflowParser.parse(buf, null, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testInvalidPacketTooShort2() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    ByteBuf buf = allocator.buffer(2);
    buf.writeShort(5);
    netflowParser.parse(buf, null, null);
  }

  @Test
  public void testV5() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    NetflowParser netflowParser = new NetflowParser(getContext());
    byte[] bytes = Files.readAllBytes(TEN_PACKETS.toPath());
    ByteBuf buf = allocator.buffer(bytes.length);
    buf.writeBytes(bytes);
    List<Record> records = netflowParser.parse(buf, null, null);
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
    assertRecord(records.get(0), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 9231, "247.193.164.155", "227.213.154.241",
      17, "2015-04-12T21:32:19.0577", "2015-04-12T21:32:19.0577", 504, 1, 0, 89);
    assertRecord(records.get(1), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 64042, "247.193.164.155", "227.213.154.241",
      17, "2015-04-12T21:32:19.0577", "2015-04-12T21:32:19.0577", 504, 1, 0, 89);
    assertRecord(records.get(2), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 18784, "247.193.164.155", "227.213.154.241",
      17, "2015-04-12T21:32:19.0577", "2015-04-12T21:32:19.0577", 504, 1, 0, 89);
    assertRecord(records.get(3), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 43998, "249.229.186.21", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertRecord(records.get(4), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 8790, "127.227.189.185", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertRecord(records.get(5), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 38811, "127.227.189.185", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertRecord(records.get(6), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 48001, "127.227.189.185", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertRecord(records.get(7), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 57572, "249.229.186.21", "227.213.154.241",
      17, "2015-04-12T21:32:19.0575", "2015-04-12T21:32:19.0575", 504, 1, 0, 89);
    assertRecord(records.get(8), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 54356, "45.103.41.119", "227.213.154.241",
      17, "2015-04-12T21:32:19.0573", "2015-04-12T21:32:19.0573", 504, 1, 0, 696);
    assertRecord(records.get(9), 5, "2b750f7c-7c25-1000-8080-808080808080", 53, 5557, "121.75.53.47", "227.213.154.241",
      17, "2015-04-12T21:32:19.0572", "2015-04-12T21:32:19.0572", 504, 1, 0, 504);
  }

  private void assertRecord(Record record, int version, String packetId, int srcport, int dstport, String srcaddr,
                            String dstaddr, int proto, String first, String last, int length, int packets, int seq,
                             int octets) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSSS");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    Map<String, Field>  map = record.get().getValueAsMap();
    Assert.assertEquals(version, map.get(NetflowParser.VERSION).getValueAsInteger());
    Assert.assertEquals(packetId, map.get(NetflowParser.PACKETID).getValueAsString());
    Assert.assertEquals(srcport, map.get(NetflowParser.SRCPORT).getValueAsInteger());
    Assert.assertEquals(dstport, map.get(NetflowParser.DSTPORT).getValueAsInteger());
    Assert.assertEquals(srcaddr, map.get(NetflowParser.SRCADDR_S).getValueAsString());
    Assert.assertEquals(dstaddr, map.get(NetflowParser.DSTADDR_S).getValueAsString());
    Assert.assertEquals(proto, map.get(NetflowParser.PROTO).getValueAsInteger());
    Assert.assertEquals(length, map.get(NetflowParser.LENGTH).getValueAsInteger());
    Assert.assertEquals(packets, map.get(NetflowParser.PACKETS).getValueAsInteger());
    Assert.assertEquals(seq, map.get(NetflowParser.FLOWSEQ).getValueAsInteger());
    Assert.assertEquals(octets, map.get(NetflowParser.DOCTECTS).getValueAsInteger());
    Assert.assertEquals(first, dateFormat.format(new Date(map.get(NetflowParser.FIRST).getValueAsLong())));
    Assert.assertEquals(last, dateFormat.format(new Date(map.get(NetflowParser.LAST).getValueAsLong())));
  }
}
