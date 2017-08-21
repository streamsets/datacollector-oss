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
package com.streamsets.pipeline.stage.destination.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.kafka.util.KafkaTargetUtil;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category(SingleForkNoReuseTest.class)
public class TestKafkaTargetDataFormats {

  private static final SdcKafkaTestUtil sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();

  private static List<KafkaStream<byte[], byte[]>> xmlStream;

  @BeforeClass
  public static void setUp() throws IOException, InterruptedException {
    sdcKafkaTestUtil.startZookeeper();
    sdcKafkaTestUtil.startKafkaBrokers(1);

    // create topics
    sdcKafkaTestUtil.createTopic("xml", 1, 1);

    TestUtils.waitUntilMetadataIsPropagated(
      scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
      "xml",
      0,
      5000
    );

    xmlStream = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), "xml", 1);
  }

  @AfterClass
  public static void tearDown() {
    sdcKafkaTestUtil.shutdown();
  }

  @Test
  public void testXML() throws InterruptedException, StageException {
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.xmlPrettyPrint = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
      sdcKafkaTestUtil.getMetadataBrokerURI(),
      "xml",
      "-1",
      sdcKafkaTestUtil.setMaxAcks(new HashMap<>()),
      false,
      PartitionStrategy.RANDOM,
      false,
      null,
      null,
      new KafkaTargetConfig(),
      DataFormat.XML,
      dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    targetRunner.runWrite(ImmutableList.of(getXmlRecord()));
    targetRunner.runDestroy();


    List<String> messages = new ArrayList<>();
    ConsumerIterator<byte[], byte[]> it = xmlStream.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }

    Assert.assertEquals(1, messages.size());
    Assert.assertEquals(
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
      "<entry>\n" +
      "  <company>StreamSets</company>\n" +
      "  <state>CA</state>\n" +
      "</entry>\n",
      messages.get(0)
    );
  }

  private Record getXmlRecord() {
    Map<String, Field> rootMap = ImmutableMap.of(
      "company", Field.create("StreamSets"),
      "state", Field.create("CA")
    );
    Record r = RecordCreator.create("s", "s:1");
    r.set(Field.create(ImmutableMap.of("entry", Field.create(rootMap))));
    return r;
  }
}
