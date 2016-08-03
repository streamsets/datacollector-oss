/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.generator.wholefile;

import com.codahale.metrics.Gauge;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.io.fileref.FileRefStreamStatisticsConstants;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

public class TestWholeFileDataGeneratorFactory {
  private Stage.Context context;

  @Before
  public void setup() throws Exception {
    context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }

  @Test
  public void testGaugeInit() throws Exception {
    DataGeneratorFactory factory =
        new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.WHOLE_FILE).build();
    Assert.assertNull(context.getGauge(FileRefStreamStatisticsConstants.GAUGE_NAME));
    factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertNotNull(context.getGauge(FileRefStreamStatisticsConstants.GAUGE_NAME));
  }

  @Test
  public void testGaugeOrdering() throws Exception {
    DataGeneratorFactory factory =
        new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.WHOLE_FILE).build();
    factory.getGenerator(new ByteArrayOutputStream());
    Gauge<Map<String, Object>> gauge = context.getGauge(FileRefStreamStatisticsConstants.GAUGE_NAME);
    Map<String, Object> map = gauge.getValue();

    LinkedHashSet<String> hashSet = new LinkedHashSet<>();
    //Ordering
    hashSet.add(FileRefStreamStatisticsConstants.FILE_NAME);
    hashSet.add(FileRefStreamStatisticsConstants.TRANSFER_THROUGHPUT);
    hashSet.add(FileRefStreamStatisticsConstants.COPIED_BYTES);
    hashSet.add(FileRefStreamStatisticsConstants.REMAINING_BYTES);

    Iterator<String> hashSetKeyIterator = hashSet.iterator();
    Iterator<String> keySetIterator = map.keySet().iterator();

    while (hashSetKeyIterator.hasNext()) {
      Assert.assertEquals(hashSetKeyIterator.next(), keySetIterator.next());
    }

    hashSetKeyIterator = hashSet.iterator();
    Iterator<Map.Entry<String, Object>> entrySetIterator = map.entrySet().iterator();
    while (hashSetKeyIterator.hasNext()) {
      Assert.assertEquals(hashSetKeyIterator.next(), entrySetIterator.next().getKey());
    }
  }
}
