/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

  private static final int TIME_OUT = 1000;
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final String CLIENT_PREFIX = "Client";
  private static final String UNDER_SCORE = "_";
  private static final long ONE_SECOND = 1000;

  /*Topic to readData from*/
  private final String topic;
  /*Topic to readData from*/
  private final int partition;
  /*Host on which the seed broker is running*/
  private final KafkaBroker broker;
  /*client id*/
  private final String clientName;
  /*The max amount of data that needs to be fetched from kafka in a single attempt*/
  private final int minFetchSize;
  /*The min amount of data that needs to be fetched from kafka in a single attempt*/
  private final int maxFetchSize;
  /*The max time to wait before returning from a kafka read operation if no message is available*/
  private final int maxWaitTime;
  /*replica brokers*/
  private List<KafkaBroker> replicaBrokers;

  private SimpleConsumer consumer;
  private KafkaBroker leader;

  public KafkaConsumer(String topic, int partition, KafkaBroker broker, int minFetchSize, int maxFetchSize, int maxWaitTime) {
    this.topic = topic;
    this.partition = partition;
    this.broker = broker;
    this.maxFetchSize = maxFetchSize;
    this.minFetchSize = minFetchSize;
    this.maxWaitTime = maxWaitTime;
    this.clientName = CLIENT_PREFIX + UNDER_SCORE + topic + UNDER_SCORE + partition;
    this.replicaBrokers = new ArrayList<>();
  }

  public void init() {
    List<KafkaBroker> brokers = new ArrayList<>();
    brokers.add(broker);
    PartitionMetadata metadata = getPartitionMetadata(brokers, topic, partition);
    if (metadata == null) {
      LOG.error("Can't find metadata for Topic '{}' and Partition '{}'.", topic, partition);
      return;
    }
    if (metadata.leader() == null) {
      LOG.error("Can't find leader for Topic '{}' and Partition '{}'.", topic, partition);
      return;
    }
    leader = new KafkaBroker(metadata.leader().host(), metadata.leader().port());
    //recreate consumer instance with the leader information for that topic
    consumer = new SimpleConsumer(leader.getHost(), leader.getPort(), TIME_OUT, BUFFER_SIZE, clientName);
  }

  public void destroy() {
    if(consumer != null) {
      consumer.close();
    }
  }

  public List<MessageAndOffset> read(long offset) throws Exception {

    FetchRequest req = buildFetchRequest(offset);
    FetchResponse fetchResponse = consumer.fetch(req);

    if(fetchResponse.hasError()) {
      //try handling the error
      short code = fetchResponse.errorCode(topic, partition);
      if(code == ErrorMapping.OffsetOutOfRangeCode()) {
        //invalid offset
        //FIXME: Does this honor at most once delivery?
        offset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
      } else {
        consumer.close();
        consumer = null;
        leader = findNewLeader(leader, topic, partition);
      }

      //re-fetch
      req = buildFetchRequest(offset);
      fetchResponse = consumer.fetch(req);

      if(fetchResponse.hasError()) {
        LOG.error("Error fetching data from kafka. Topic '{}', Partition '{}', Offset '{}.", topic, partition, offset);
        //
      }
    }

    List<MessageAndOffset> partitionToPayloadMapArrayList = new ArrayList<>();
    int numberOfMessagesRead = 0;
    for (kafka.message.MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
      long currentOffset = messageAndOffset.offset();
      if (currentOffset < offset) {
        LOG.warn("Found old offset '{}'. Expected offset '{}'. Discarding message", currentOffset, offset);
        continue;
      }
      ByteBuffer payload = messageAndOffset.message().payload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      MessageAndOffset partitionToPayloadMap = new MessageAndOffset(bytes, messageAndOffset.nextOffset());
      partitionToPayloadMapArrayList.add(partitionToPayloadMap);
      numberOfMessagesRead++;
    }

    if(numberOfMessagesRead == 0) {
      //If no message is available, give kafka sometime.
      try {
        Thread.sleep(ONE_SECOND);
      } catch (InterruptedException e) {
      }
    }
    return partitionToPayloadMapArrayList;
  }


  public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                   long whichTime, String clientName) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
      requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      LOG.error("Error fetching offset data from the Broker '{}' : {}", consumer.host(),
        response.errorCode(topic, partition) );
      return 0;
    }
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }

  private KafkaBroker findNewLeader(KafkaBroker oldLeader, String topic, int partition) throws Exception {
    for (int i = 0; i < 3; i++) {
      boolean goToSleep;
      PartitionMetadata metadata = getPartitionMetadata(replicaBrokers, topic, partition);
      if (metadata == null) {
        goToSleep = true;
      } else if (metadata.leader() == null) {
        goToSleep = true;
      } else if (oldLeader.getHost().equalsIgnoreCase(metadata.leader().host()) && i == 0) {
        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        //
        goToSleep = true;
      } else {
        return new KafkaBroker(metadata.leader().host(), metadata.leader().port());
      }
      if (goToSleep) {
        try {
          Thread.sleep(ONE_SECOND);
        } catch (InterruptedException ie) {
        }
      }
    }
    LOG.error("Unable to find new leader after Broker failure. Exiting");
    //TODO: Fix exception
    throw new Exception("Unable to find new leader after Broker failure. Exiting");
  }

  private PartitionMetadata getPartitionMetadata(List<KafkaBroker> brokers, String topic, int partition) {
    PartitionMetadata returnMetaData = null;
    for(KafkaBroker broker : brokers) {
      try {
        consumer = new SimpleConsumer(broker.getHost(), broker.getPort(), TIME_OUT, BUFFER_SIZE, clientName);
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

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
        LOG.error("Error communicating with Broker '{}' to find leader for topic '{}' partition '{}'. Reason : {}",
          broker.getHost() + ":" + broker.getPort(), topic, partition, e.getMessage());

      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }
    if (returnMetaData != null) {
      replicaBrokers.clear();
      for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
        replicaBrokers.add(new KafkaBroker(replica.host(), replica.port()));
      }
    }
    return returnMetaData;
  }

  public long getOffsetToRead(boolean fromBeginning) {
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
    return new FetchRequestBuilder()
      .clientId(clientName)
      .minBytes(minFetchSize)
      .maxWait(maxWaitTime)
      .addFetch(topic, partition, offset, maxFetchSize)
      .build();
  }
}
