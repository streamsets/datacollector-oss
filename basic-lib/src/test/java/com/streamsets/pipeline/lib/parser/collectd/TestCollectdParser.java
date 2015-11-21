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
package com.streamsets.pipeline.lib.parser.collectd;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestCollectdParser {
  private static final Logger LOG = LoggerFactory.getLogger(TestCollectdParser.class);
  private static final Charset CHARSET = StandardCharsets.UTF_8;

  // Common Record Values
  private static final Field HOST = Field.create("ip-192-168-42-24.us-west-2.compute.internal");
  private static final Field INTERVAL_10S = Field.create(10737418240L);

  // Record Values
  private static final Map<String, Field> expectedRecord0 = new ImmutableMap.Builder<String, Field>()
      .put("host", HOST)
      .put("interval_hires", INTERVAL_10S)
      .put("time_hires", Field.create(1543270991079203521L))
      .put("plugin", Field.create("cpu"))
      .put("plugin_instance", Field.create("7"))
      .put("type", Field.create("cpu"))
      .put("type_instance", Field.create("nice"))
      .put("value", Field.create(0L))
      .build();

  private static final Map<String, Field> expectedRecord2 = new ImmutableMap.Builder<String, Field>()
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

  private static final Map<String, Field> expectedRecordNoInterval0 = new ImmutableMap.Builder<String, Field>()
      .put("host", HOST)
      .put("time_hires", Field.create(1543270991079203521L))
      .put("plugin", Field.create("cpu"))
      .put("plugin_instance", Field.create("7"))
      .put("type", Field.create("cpu"))
      .put("type_instance", Field.create("nice"))
      .put("value", Field.create(0L))
      .build();

  private static final Map<String, Field> expectedRecordNoInterval2 = new ImmutableMap.Builder<String, Field>()
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

  private static final Map<String, Field> encryptedRecord14 = new ImmutableMap.Builder<String, Field>()
      .put("host", ENC_HOST)
      .put("interval_hires", INTERVAL_10S)
      .put("time_hires", Field.create(1543510262623895761L))
      .put("plugin", Field.create("interface"))
      .put("plugin_instance", Field.create("en8"))
      .put("type", Field.create("if_octets"))
      .put("tx", Field.create(216413106L))
      .put("rx", Field.create(1324428131L))
      .build();

  private static final Map<String, Field> signedRecord15 = new ImmutableMap.Builder<String, Field>()
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

  private static final File SINGLE_PACKET = new File(System.getProperty("user.dir") +
      "/src/test/resources/collectd23part.bin");

  private static final File SINGLE_ENCRYPTED_PACKET = new File(System.getProperty("user.dir") +
      "/src/test/resources/collectd_encrypted.bin");

  private static final File SINGLE_SIGNED_PACKET = new File(System.getProperty("user.dir") +
      "/src/test/resources/collectd_signed.bin");

  private static final String AUTH_FILE_PATH = System.getProperty("user.dir") +
      "/src/test/resources/collectd_auth.txt";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
        Collections.<String>emptyList());
  }

  @Test
  public void testParser() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    CollectdParser parser = new CollectdParser(getContext(), false, null, false, null, CHARSET);
    byte[] bytes = Files.readAllBytes(SINGLE_PACKET.toPath());
    ByteBuf buf = allocator.buffer(bytes.length);
    buf.writeBytes(bytes);
    List<Record> records = parser.parse(buf, null, null);

    Assert.assertEquals(23, records.size()); // 23 Value parts

    Record record0 = records.get(0);
    verifyRecord(expectedRecord0, record0);

    Record record2 = records.get(2);
    verifyRecord(expectedRecord2, record2);

  }

  @Test
  public void testParserExcludeInterval() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    CollectdParser parser = new CollectdParser(getContext(), false, null, true, null, CHARSET);
    byte[] bytes = Files.readAllBytes(SINGLE_PACKET.toPath());
    ByteBuf buf = allocator.buffer(bytes.length);
    buf.writeBytes(bytes);
    List<Record> records = parser.parse(buf, null, null);

    Assert.assertEquals(23, records.size()); // 23 Value parts

    Record record0 = records.get(0);
    verifyRecord(expectedRecordNoInterval0, record0);

    Record record2 = records.get(2);
    verifyRecord(expectedRecordNoInterval2, record2);

  }

  @Test
  public void testEncryptedRecord() throws Exception {
    // If unlimited strength encryption is not available, we cant run this test.
    Assume.assumeFalse(Cipher.getMaxAllowedKeyLength("AES") < 256);

    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    CollectdParser parser = new CollectdParser(getContext(), false, null, false, AUTH_FILE_PATH, CHARSET);
    byte[] bytes = Files.readAllBytes(SINGLE_ENCRYPTED_PACKET.toPath());
    ByteBuf buf = allocator.buffer(bytes.length);
    buf.writeBytes(bytes);
    List<Record> records = parser.parse(buf, null, null);

    Assert.assertEquals(24, records.size()); // 24 value parts
    Record record14 = records.get(14);
    verifyRecord(encryptedRecord14, record14);
    LOG.info("Num records: {}", records.size());
  }

  @Test
  public void testSignedRecord() throws Exception {
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    CollectdParser parser = new CollectdParser(getContext(), false, null, false, AUTH_FILE_PATH, CHARSET);
    byte[] bytes = Files.readAllBytes(SINGLE_SIGNED_PACKET.toPath());
    ByteBuf buf = allocator.buffer(bytes.length);
    buf.writeBytes(bytes);
    List<Record> records = parser.parse(buf, null, null);

    Assert.assertEquals(22, records.size()); // 22 value parts
    Record record15 = records.get(15);
    verifyRecord(signedRecord15, record15);
    LOG.info("Num records: {}", records.size());
  }

  private void verifyRecord(Map<String, Field> expected, Record record) {
    Map<String, Field> actual = record.get("/").getValueAsMap();

    Assert.assertEquals(expected.size(), actual.size());

    Set<Map.Entry<String, Field>> difference = Sets.difference(expected.entrySet(), actual.entrySet());
    Assert.assertEquals(true, difference.isEmpty());
  }
}
