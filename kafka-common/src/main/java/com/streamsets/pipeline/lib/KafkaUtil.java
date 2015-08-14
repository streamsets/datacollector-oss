/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
  private static final String METADATA_READER_CLIENT = "metadataReaderClient";
  private static final int METADATA_READER_TIME_OUT = 10000;
  private static final int BUFFER_SIZE = 64 * 1024;

  public static TopicMetadata getTopicMetadata(List<KafkaBroker> kafkaBrokers, String topic, int maxRetries,
                                               long backOffms) throws IOException {
    TopicMetadata topicMetadata = null;
    boolean connectionError = true;
    boolean retry = true;
    int retryCount = 0;
    while (retry && retryCount <= maxRetries) {
      for (KafkaBroker broker : kafkaBrokers) {
        SimpleConsumer simpleConsumer = null;
        try {
          simpleConsumer = new SimpleConsumer(broker.getHost(), broker.getPort(), METADATA_READER_TIME_OUT, BUFFER_SIZE,
            METADATA_READER_CLIENT);

          List<String> topics = Collections.singletonList(topic);
          TopicMetadataRequest req = new TopicMetadataRequest(topics);
          kafka.javaapi.TopicMetadataResponse resp = simpleConsumer.send(req);

          // No exception => no connection error
          connectionError = false;

          List<TopicMetadata> topicMetadataList = resp.topicsMetadata();
          if (topicMetadataList == null || topicMetadataList.isEmpty()) {
            //This broker did not have any metadata. May not be in sync?
            continue;
          }
          topicMetadata = topicMetadataList.iterator().next();
          if (topicMetadata != null && topicMetadata.errorCode() == 0) {
            retry = false;
          }
        } catch (Exception e) {
          //could not connect to this broker, try others
        } finally {
          if (simpleConsumer != null) {
            simpleConsumer.close();
          }
        }
      }
      if(retry) {
        LOG.warn("Unable to connect or cannot fetch topic metadata. Waiting for '{}' seconds before retrying",
          backOffms/1000);
        retryCount++;
        if(!ThreadUtil.sleep(backOffms)) {
          break;
        }
      }
    }
    if(connectionError) {
      //could not connect any broker even after retries. Fail with exception
      throw new IOException(Utils.format(KafkaErrors.KAFKA_67.getMessage(), getKafkaBrokers(kafkaBrokers)));
    }
    return topicMetadata;
  }

  public static int getPartitionCount(String metadataBrokerList, String topic, int maxRetries,
                                               long backOffms) throws IOException {
    List<KafkaBroker> kafkaBrokers = getKafkaBrokers(metadataBrokerList);
    if(kafkaBrokers.isEmpty()) {
      throw new IOException(Utils.format(KafkaErrors.KAFKA_07.getMessage(), metadataBrokerList));
    }
    TopicMetadata topicMetadata = getTopicMetadata(kafkaBrokers, topic, maxRetries, backOffms);
    if(topicMetadata == null || topicMetadata.errorCode() != 0) {
      throw new IOException(Utils.format(KafkaErrors.KAFKA_03.getMessage(), topic, metadataBrokerList));
    }
    return topicMetadata.partitionsMetadata().size();
  }

  public static List<KafkaBroker> validateKafkaBrokerConnectionString(List<Stage.ConfigIssue> issues, String connectionString,
                                                                      String configGroupName, String configName, Stage.Context context) {
    if(connectionString == null || connectionString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
        KafkaErrors.KAFKA_06, configName));
      return null;
    }
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    String[] brokers = connectionString.split(",");
    for(String broker : brokers) {
      String[] brokerHostAndPort = broker.split(":");
      if(brokerHostAndPort.length != 2) {
        issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_07, connectionString));
      } else {
        try {
          int port = Integer.parseInt(brokerHostAndPort[1].trim());
          kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0].trim(), port));
        } catch (NumberFormatException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_07, connectionString));
        }
      }
    }
    return kafkaBrokers;
  }

  public static List<KafkaBroker> validateZkConnectionString(List<Stage.ConfigIssue> issues, String connectString,
                                                             String configGroupName, String configName,
                                                             Stage.Context context) {
    if(connectString == null || connectString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
        KafkaErrors.KAFKA_06, configName));
      return null;
    }

    String chrootPath;
    int off = connectString.indexOf('/');
    if (off >= 0) {
      chrootPath = connectString.substring(off);
      // ignore a single "/". Same as null. Anything longer must be validated
      if (chrootPath.length() > 1) {
        try {
          PathUtils.validatePath(chrootPath);
        } catch (IllegalArgumentException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_09, connectString,
            e.toString()));
        }
      }
      connectString = connectString.substring(0, off);
    } else {
      //do nothing
    }

    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    String brokers[] = connectString.split(",");
    for(String broker : brokers) {
      String[] brokerHostAndPort = broker.split(":");
      if(brokerHostAndPort.length != 2) {
        issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_09, connectString,
          "The connection String is not of the form <HOST>:<PORT>"));
      } else {
        try {
          int port = Integer.parseInt(brokerHostAndPort[1].trim());
          kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0].trim(), port));
        } catch (NumberFormatException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_07, connectString,
            e.toString()));
        }
      }
    }
    return kafkaBrokers;
  }

  public static String getKafkaBrokers(List<KafkaBroker> kafkaBrokers) {
    StringBuilder sb = new StringBuilder();
    for(KafkaBroker k : kafkaBrokers) {
      sb.append(k.getHost() + ":" + k.getPort()).append(", ");
    }
    sb.setLength(sb.length()-2);
    return sb.toString();
  }

  public static List<KafkaBroker> getKafkaBrokers(String brokerList) {
    List<KafkaBroker> kafkaBrokers = new ArrayList<>();
    if(brokerList != null && !brokerList.isEmpty()) {
      String[] brokers = brokerList.split(",");
      for (String broker : brokers) {
        String[] brokerHostAndPort = broker.split(":");
        if (brokerHostAndPort.length == 2) {
          try {
            int port = Integer.parseInt(brokerHostAndPort[1].trim());
            kafkaBrokers.add(new KafkaBroker(brokerHostAndPort[0].trim(), port));
          } catch (NumberFormatException e) {
            //ignore broker
          }
        }
      }
    }
    return kafkaBrokers;
  }
}
