/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.kafka.impl;

import com.google.common.net.HostAndPort;
import com.mapr.streams.Admin;
import com.mapr.streams.Streams;
import com.mapr.db.exceptions.TableNotFoundException;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.maprstreams.MapRStreamsErrors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapR52StreamsValidationUtil09 extends MapRStreamsValidationUtil09  {

  private static final Logger LOG = LoggerFactory.getLogger(MapR52StreamsValidationUtil09.class);
  private final Set<String> streamCache = new HashSet<>();

  @Override
  public boolean validateTopicExistence(
    Stage.Context context,
    String groupName,
    String configName,
    List<HostAndPort> kafkaBrokers,
    String metadataBrokerList,
    String topic,
    Map<String, Object> kafkaClientConfigs,
    List<Stage.ConfigIssue> issues,
    boolean producer
  ) {
    boolean valid = true;
    if(topic == null || topic.isEmpty()) {
      issues.add(context.createConfigIssue(groupName, configName, KafkaErrors.KAFKA_05));
      valid = false;
    } else {
      List<PartitionInfo> partitionInfos;
      try {
        // Use KafkaConsumer to check the topic existance
        // Using KafkaProducer causes creating a topic if not exist, even if runtime topic resolution is set to false.
        KafkaConsumer<String, String> kafkaConsumer = createTopicMetadataClient();
        partitionInfos = kafkaConsumer.partitionsFor(topic);
        if (null == partitionInfos || partitionInfos.isEmpty()) {
          issues.add(
              context.createConfigIssue(
                  groupName,
                  KAFKA_CONFIG_BEAN_PREFIX + "topic",
                  MapRStreamsErrors.MAPRSTREAMS_02,
                  topic
              )
          );
          valid = false;
        }
      } catch (KafkaException e) {
        LOG.error(MapRStreamsErrors.MAPRSTREAMS_01.getMessage(), topic, e.toString(), e);
        issues.add(
            context.createConfigIssue(
                groupName,
                configName,
                MapRStreamsErrors.MAPRSTREAMS_01,
                topic,
                e.getMessage()
            )
        );
        valid = false;
      }
    }
    return valid;
  }

  /**
   * Should be called only by MapR Streams Producer. It creates a topic using KafkaProducer.
   * @param topic
   * @param kafkaClientConfigs
   * @param metadataBrokerList
   * @throws StageException
   */
  @Override
  public void createTopicIfNotExists(String topic, Map<String, Object> kafkaClientConfigs, String metadataBrokerList)
      throws StageException {
    // check stream path and topic
    if (topic.startsWith("/") && topic.contains(":")) {
      String[] path = topic.split(":");
      if (path.length != 2) {
        // Stream topic has invalid format. Record will be sent to error
        throw new StageException(MapRStreamsErrors.MAPRSTREAMS_21, topic);
      }
      String streamPath = path[0];

      if (!streamCache.contains(streamPath)) {
        // This pipeline sees this stream path for the 1st time
        Configuration conf = new Configuration();
        kafkaClientConfigs.forEach((k, v) -> {
          conf.set(k, v.toString());
        });
        Admin streamAdmin = null;
        try {
          streamAdmin = Streams.newAdmin(conf);
          // Check if the stream path exists already
          streamAdmin.countTopics(streamPath);
          streamCache.add(streamPath);
        } catch (TableNotFoundException e) {
          LOG.debug("Stream not found. Creating a new stream: " + streamPath);
          try {
            streamAdmin.createStream(streamPath, Streams.newStreamDescriptor());
            streamCache.add(streamPath);
          } catch (IOException ioex) {
            throw new StageException(MapRStreamsErrors.MAPRSTREAMS_22, streamPath, e.getMessage(), e);
          }
        } catch (IOException | IllegalArgumentException e) {
          throw new StageException(MapRStreamsErrors.MAPRSTREAMS_23, e.getMessage(), e);
        } finally {
          if (streamAdmin != null) {
            streamAdmin.close();
          }
        }
      }
    }

    // Stream topic can be created through KafkaProducer if Stream Path exists already
    KafkaProducer<String, String> kafkaProducer = createProducerTopicMetadataClient(kafkaClientConfigs);
    kafkaProducer.partitionsFor(topic);
  }

}
