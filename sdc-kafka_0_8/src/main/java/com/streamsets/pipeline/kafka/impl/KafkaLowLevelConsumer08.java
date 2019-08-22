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
package com.streamsets.pipeline.kafka.impl;

import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.SdcKafkaLowLevelConsumer;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaLowLevelConsumer08 implements SdcKafkaLowLevelConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaLowLevelConsumer08.class);

  private static final int METADATA_READER_TIME_OUT = 10000;
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final long ONE_SECOND = 1000;
  private static final String METADATA_READER_CLIENT = "metadataReaderClient";

  /*Topic to readData from*/
  private final String topic;
  /*Topic to readData from*/
  private final int partition;
  /*Host on which the seed broker is running*/
  private final HostAndPort broker;
  /*client id or consumer group id*/
  private final String clientName;
  /*The max amount of data that needs to be fetched from kafka in a single attempt*/
  private final int minFetchSize;
  /*The min amount of data that needs to be fetched from kafka in a single attempt*/
  private final int maxFetchSize;
  /*The max time to wait before returning from a kafka read operation if no message is available*/
  private final int maxWaitTime;
  /*replica brokers*/
  private List<HostAndPort> replicaBrokers;

  private SimpleConsumer consumer;
  private HostAndPort leader;

  public KafkaLowLevelConsumer08(
      String topic,
      int partition,
      HostAndPort broker,
      int minFetchSize,
      int maxFetchSize,
      int maxWaitTime,
      String clientName
  ) {
    this.topic = topic;
    this.partition = partition;
    this.broker = broker;
    this.maxFetchSize = maxFetchSize;
    this.minFetchSize = minFetchSize;
    this.maxWaitTime = maxWaitTime;
    this.clientName = clientName;
    this.replicaBrokers = new ArrayList<>();
  }

  @Override
  public void init() throws StageException {
    List<HostAndPort> brokers = new ArrayList<>();
    brokers.add(broker);
    PartitionMetadata metadata = getPartitionMetadata(brokers, topic, partition);
    if (metadata == null) {
      LOG.error(KafkaErrors.KAFKA_23.getMessage(), topic, partition);
      throw new StageException(KafkaErrors.KAFKA_23, topic, partition);
    }
    if (metadata.leader() == null) {
      LOG.error(KafkaErrors.KAFKA_24.getMessage(), topic, partition);
      throw new StageException(KafkaErrors.KAFKA_24, topic, partition);
    }
    leader = HostAndPort.fromParts(metadata.leader().host(), metadata.leader().port());
    //recreate consumer instance with the leader information for that topic
    LOG.info(
        "Creating SimpleConsumer using the following configuration: host {}, port {}, max wait time {}, max " +
        "fetch size {}, client columnName {}",
        leader.getHostText(),
        leader.getPort(),
        maxWaitTime,
        maxFetchSize,
        clientName
    );
    consumer = new SimpleConsumer(
        leader.getHostText(),
        leader.getPort(),
        maxWaitTime,
        maxFetchSize,
        clientName
    );
  }

  @Override
  public void destroy() {
    if(consumer != null) {
      consumer.close();
    }
  }

  @Override
  public String getVersion() {
    return Kafka08Constants.KAFKA_VERSION;
  }

  @Override
  public List<MessageAndOffset> read(long offset) throws StageException {

    FetchRequest req = buildFetchRequest(offset);
    FetchResponse fetchResponse;
    try {
      fetchResponse = consumer.fetch(req);
    } catch (Exception e) {
      if(e instanceof SocketTimeoutException) {
        //If the value of consumer.timeout.ms is set to a positive integer, a timeout exception is thrown to the
        //consumer if no message is available for consumption after the specified timeout value.
        //If this happens exit gracefully
        LOG.warn(KafkaErrors.KAFKA_28.getMessage());
        return Collections.emptyList();
      } else {
        throw new StageException(KafkaErrors.KAFKA_29, e.toString(), e);
      }
    }

    if(fetchResponse.hasError()) {
      short code = fetchResponse.errorCode(topic, partition);
      if(code == ErrorMapping.OffsetOutOfRangeCode()) {
        //invalid offset
        offset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
      } else {
        //try re-initializing connection with kafka
        consumer.close();
        consumer = null;
        leader = findNewLeader(leader, topic, partition);
      }

      //re-fetch
      req = buildFetchRequest(offset);
      fetchResponse = consumer.fetch(req);

      if(fetchResponse.hasError()) {
        //could not fetch the second time, give kafka some time
        LOG.error(KafkaErrors.KAFKA_26.getMessage(), topic, partition, offset);
      }
    }

    List<MessageAndOffset> partitionToPayloadMapArrayList = new ArrayList<>();
    for (kafka.message.MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
      long currentOffset = messageAndOffset.offset();
      if (currentOffset < offset) {
        LOG.warn(KafkaErrors.KAFKA_27.getMessage(), currentOffset, offset);
        continue;
      }
      ByteBuffer payload = messageAndOffset.message().payload();
      final Object key = messageAndOffset.message().key();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      MessageAndOffset partitionToPayloadMap = new MessageAndOffset(
          key,
          bytes,
          messageAndOffset.nextOffset(),
          partition
      );
      partitionToPayloadMapArrayList.add(partitionToPayloadMap);
    }
    return partitionToPayloadMapArrayList;
  }


  private long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                   long whichTime, String clientName) throws StageException {
    try {
      TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
      kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
      OffsetResponse response = consumer.getOffsetsBefore(request);

      if (response.hasError()) {
        LOG.error(KafkaErrors.KAFKA_22.getMessage(), consumer.host() + ":" + consumer.port(),
          response.errorCode(topic, partition));
        return 0;
      }
      long[] offsets = response.offsets(topic, partition);
      return offsets[0];
    } catch (Exception e) {
      LOG.error(KafkaErrors.KAFKA_30.getMessage(), e.toString(), e);
      throw new StageException(KafkaErrors.KAFKA_30, e.toString(), e);
    }
  }

  private HostAndPort findNewLeader(HostAndPort oldLeader, String topic, int partition) throws StageException {
    //try 3 times to find a new leader
    for (int i = 0; i < 3; i++) {
      boolean sleep;
      PartitionMetadata metadata = getPartitionMetadata(replicaBrokers, topic, partition);
      if (metadata == null || metadata.leader() == null) {
        sleep = true;
      } else if (oldLeader.getHostText().equalsIgnoreCase(metadata.leader().host()) && i == 0) {
        //leader has not yet changed, give zookeeper sometime
        sleep = true;
      } else {
        return HostAndPort.fromParts(metadata.leader().host(), metadata.leader().port());
      }
      if (sleep) {
        ThreadUtil.sleep(ONE_SECOND);
      }
    }
    LOG.error(KafkaErrors.KAFKA_21.getMessage());
    throw new StageException(KafkaErrors.KAFKA_21);
  }

  private PartitionMetadata getPartitionMetadata(List<HostAndPort> brokers, String topic, int partition) {
    PartitionMetadata returnMetaData = null;
    for(HostAndPort broker : brokers) {
      SimpleConsumer simpleConsumer = null;
      try {
        LOG.info("Creating SimpleConsumer using the following configuration: host {}, port {}, max wait time {}, max " +
          "fetch size {}, client columnName {}", broker.getHostText(), broker.getPort(), METADATA_READER_TIME_OUT, BUFFER_SIZE,
          METADATA_READER_CLIENT);
        simpleConsumer = new SimpleConsumer(broker.getHostText(), broker.getPort(), METADATA_READER_TIME_OUT, BUFFER_SIZE,
          METADATA_READER_CLIENT);

        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = simpleConsumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == partition) {
              returnMetaData = part;
              break;
            }
          }
        }
      } catch (Exception e) {
        LOG.error(KafkaErrors.KAFKA_25.getMessage(), broker.toString(), topic, partition,
          e.toString(), e);
      } finally {
        if (simpleConsumer != null) {
          simpleConsumer.close();
        }
      }
    }
    if (returnMetaData != null) {
      replicaBrokers.clear();
      for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
        replicaBrokers.add(HostAndPort.fromParts(replica.host(), replica.port()));
      }
    }
    return returnMetaData;
  }

  @Override
  public long getOffsetToRead(boolean fromBeginning) throws StageException {
    long whichTime = kafka.api.OffsetRequest.LatestTime();
    if (fromBeginning) {
      whichTime = kafka.api.OffsetRequest.EarliestTime();
    }
    return getLastOffset(consumer, topic, partition, whichTime, clientName);
  }


  private FetchRequest buildFetchRequest(long offset) {
    //1. maxWaitTime is the maximum amount of time in milliseconds to block waiting if insufficient data is
    //   available at the time the request is issued.

    //2. minFetchSize is the minimum number of bytes of messages that must be available to give a response. If the
    //   client sets this to 0 the server will always respond immediately, however if there is no new data since their
    //   last request they will just get back empty message sets. If this is set to 1, the server will respond as soon
    //   as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher
    //   values in combination with the timeout the consumer can tune for throughput and trade a little additional
    //   latency for reading only large chunks of data (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k
    //   would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).

    //3. maxFetchSize is the maximum bytes to include in the message set for this partition.
    //   This helps bound the size of the response.
    LOG.info("Building fetch request with clientId {}, minBytes {}, maxWait {}, topic {}, partition {}, offset {}, " +
      "max fetch size {}.", clientName, minFetchSize, maxWaitTime, topic, partition, offset, maxFetchSize);
    return new FetchRequestBuilder()
      .clientId(clientName)
      .minBytes(minFetchSize)
      .maxWait(maxWaitTime)
      .addFetch(topic, partition, offset, maxFetchSize)
      .build();
  }
}
