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
package com.streamsets.pipeline.lib.parser.udp.collectd;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.util.UDPTestUtil;
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

public class TestCollectdParser {
  private static final Logger LOG = LoggerFactory.getLogger(TestCollectdParser.class);
  private static final Charset CHARSET = StandardCharsets.UTF_8;

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
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.expectedRecord0, record0);

    Record record2 = records.get(2);
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.expectedRecord2, record2);

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
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.expectedRecordNoInterval0, record0);

    Record record2 = records.get(2);
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.expectedRecordNoInterval2, record2);

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
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.encryptedRecord14, record14);
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
    UDPTestUtil.verifyCollectdRecord(UDPTestUtil.signedRecord15, record15);
    LOG.info("Num records: {}", records.size());
  }

}
