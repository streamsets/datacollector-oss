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
package com.streamsets.pipeline.spark;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.kafka.v09.HasOffsetRanges;
import org.apache.spark.streaming.kafka.v09.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class MaprStreamsOffsetManagerImpl implements KafkaOffsetManager {
  private static final Logger LOG = LoggerFactory.getLogger(MaprStreamsOffsetManagerImpl.class);
  private static final MaprStreamsOffsetManagerImpl INSTANCE = new MaprStreamsOffsetManagerImpl();

  private MaprStreamsOffsetManagerImpl() {}

  public static MaprStreamsOffsetManagerImpl get() {
    return INSTANCE;
  }

  private static Map<Integer, Long> getOffsetToSave(OffsetRange[] offsetRanges) {
    Map<Integer, Long> partitionToOffset = new LinkedHashMap<>();
    for (int i = 0; i < offsetRanges.length; i++) {
      //Until offset is the offset till which the current SparkDriverFunction
      //Will read
      partitionToOffset.put(offsetRanges[i].partition(), offsetRanges[i].untilOffset());
      LOG.info(
          "Offset Range From RDD - Partition:{}, From Offset:{}, Until Offset:{}",
          offsetRanges[i].partition(),
          offsetRanges[i].fromOffset(),
          offsetRanges[i].untilOffset()
      );
    }
    return partitionToOffset;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void saveOffsets(RDD<?> rdd) {
    MapRStreamingBinding.offsetHelper.saveOffsets(getOffsetToSave(((HasOffsetRanges) rdd).offsetRanges()));
  }

  private Map<Integer, Long> readOffsets(int numberOfPartitions) {
    return MapRStreamingBinding.offsetHelper.readOffsets(numberOfPartitions);
  }

  @Override
  public Map<TopicPartition, Long> getOffsetForDStream(String topic, int numberOfPartitions) {
    Map<TopicPartition, Long> offsetForDStream = new HashMap<>();
    Map<Integer, Long> partitionsToOffset = readOffsets(numberOfPartitions);
    for (Map.Entry<Integer, Long> partitionAndOffset : partitionsToOffset.entrySet()) {
      offsetForDStream.put(new TopicPartition(topic, partitionAndOffset.getKey()), partitionAndOffset.getValue());
    }
    return offsetForDStream;
  }

}
