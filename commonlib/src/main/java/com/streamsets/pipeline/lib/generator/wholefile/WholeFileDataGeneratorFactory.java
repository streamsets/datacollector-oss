/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.generator.wholefile;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class WholeFileDataGeneratorFactory extends DataGeneratorFactory {
  public static final Map<String, Object> CONFIGS = new HashMap<>();
  public static final Set<Class<? extends Enum>> MODES = ImmutableSet.of();
  private static final Map<String, Integer> GAUGE_MAP_ORDERING
      = new ImmutableMap.Builder<String, Integer>()
      .put(FileRefUtil.FILE, 1)
      .put(FileRefUtil.TRANSFER_THROUGHPUT, 2)
      .put(FileRefUtil.SENT_BYTES, 3)
      .put(FileRefUtil.REMAINING_BYTES, 4)
      .put(FileRefUtil.COMPLETED_FILE_COUNT, 5)
      .build();

  public static final String INCLUDE_CHECKSUM_IN_THE_EVENTS_KEY = "includeChecksumInTheEvents";
  public static final String CHECKSUM_ALGO_KEY = "checksumAlgorithm";

  static {
    CONFIGS.put(INCLUDE_CHECKSUM_IN_THE_EVENTS_KEY, false);
    CONFIGS.put(CHECKSUM_ALGO_KEY, ChecksumAlgorithm.MD5);
  }

  /**
   * Creates a gauge if it is already not. This is done only once for the stage
   * @param context the {@link com.streamsets.pipeline.api.Stage.Context} of this stage
   */
  @SuppressWarnings("unchecked")
  private void initMetricsIfNeeded(Stage.Context context) {
    Gauge<Map<String, Object>> gauge = context.getGauge(FileRefUtil.GAUGE_NAME);
    if (gauge == null) {
      //Concurrent because the metrics thread will access this.
      final Map<String, Object> gaugeStatistics = new ConcurrentSkipListMap<>(new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          return GAUGE_MAP_ORDERING.get(o1).compareTo(GAUGE_MAP_ORDERING.get(o2));
        }
      });
      //File name is populated at the MetricEnabledWrapperStream.
      gaugeStatistics.put(FileRefUtil.FILE, "");
      gaugeStatistics.put(FileRefUtil.TRANSFER_THROUGHPUT, 0L);
      gaugeStatistics.put(FileRefUtil.SENT_BYTES, String.format(FileRefUtil.BRACKETED_TEMPLATE, 0, 0));
      gaugeStatistics.put(FileRefUtil.REMAINING_BYTES, 0L);
      gaugeStatistics.put(FileRefUtil.COMPLETED_FILE_COUNT, 0L);
      context.createGauge(FileRefUtil.GAUGE_NAME, new Gauge<Map<String, Object>>() {
        @Override
        public Map<String, Object> getValue() {
          return gaugeStatistics;
        }
      });
    }

    Meter dataTransferMeter = context.getMeter(FileRefUtil.TRANSFER_THROUGHPUT_METER);
    if (dataTransferMeter == null) {
      context.createMeter(FileRefUtil.TRANSFER_THROUGHPUT_METER);
    }
  }

  public WholeFileDataGeneratorFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataGenerator getGenerator(OutputStream os, StreamCloseEventHandler<?> streamCloseEventHandler) throws IOException {
    initMetricsIfNeeded(getSettings().getContext());
    return new WholeFileDataGenerator(
        getSettings().getContext(),
        os,
        (boolean) getSettings().getConfig(INCLUDE_CHECKSUM_IN_THE_EVENTS_KEY),
        (ChecksumAlgorithm) getSettings().getConfig(CHECKSUM_ALGO_KEY),
        streamCloseEventHandler
    );
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return getGenerator(os, null);
  }
}
