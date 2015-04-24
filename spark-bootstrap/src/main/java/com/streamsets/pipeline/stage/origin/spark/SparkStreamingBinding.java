/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;
import org.apache.spark.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import kafka.serializer.DefaultDecoder;

public class SparkStreamingBinding {
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingBinding.class);
  public static final String INPUT_TYPE = "streamsets.cluster.input.type";
  public static final String HDFS_INPUT_TYPE = "hdfs-dir";
  public static final String KAFKA_INPUT_TYPE = "kafka";

  // for tests
  public static final String TEXT_SERVER_INPUT_TYPE = "text-server";
  public static final String TEXT_SERVER_HOSTNAME = "streamsets.cluster.text.server.host";
  public static final String TEXT_SERVER_PORT = "streamsets.cluster.text.server.port";

  private JavaStreamingContext ssc;
  private Properties properties;
  private String pipelineJson;

  public SparkStreamingBinding(Properties properties, String pipelineJson) {
    this.properties = properties;
    this.pipelineJson = pipelineJson;
  }

  public void init() throws Exception {
    SparkConf conf = new SparkConf().setAppName("StreamSets Data Collector");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    ssc = new JavaStreamingContext(conf, new Duration(10000));
    final Thread shutdownHookThread = new Thread("Spark.shutdownHook") {
      @Override
      public void run() {
        LOG.debug("Gracefully stopping Spark Streaming Application");
        ssc.stop(true, true);
        LOG.info("Application stopped");
      }
    };
    Runtime.getRuntime().addShutdownHook(shutdownHookThread);

    String inputType = properties.getProperty(INPUT_TYPE);

    if (KAFKA_INPUT_TYPE.equalsIgnoreCase(inputType)) {
      JavaPairInputDStream<byte[], byte[]> dStream = createDirectStreamForKafka();
      dStream.foreachRDD(new SparkKafkaDriverFunction(properties, pipelineJson));
    } else {
      JavaDStream<String> dStream;
      if (TEXT_SERVER_INPUT_TYPE.equalsIgnoreCase(inputType)) {
        String textServerHostname = properties.getProperty(TEXT_SERVER_HOSTNAME);
        int textServerPort = Integer.parseInt(properties.getProperty(TEXT_SERVER_PORT));
        dStream = ssc.socketTextStream(textServerHostname, textServerPort);
      } else if (HDFS_INPUT_TYPE.equalsIgnoreCase(inputType)) {
        dStream = ssc.textFileStream("hdfs://node00.local:8020/user/ec2-user/input");
      } else {
        throw new IllegalStateException("Unknown input type: " + inputType);
      }
      dStream.foreachRDD(new SparkDriverFunction(properties, pipelineJson));
    }
    ssc.start();
  }

  private JavaPairInputDStream<byte[], byte[]> createDirectStreamForKafka() {


    // If using the createStream(High level Kafka consumer API) - doesn't have access to metadata
    // so use new API
    /*
    props.put("zookeeper.connect", properties.getProperty("zookeeperConnect"));
    props.put("group.id", properties.getProperty("consumerGroup"));
    topics.put("topic name", <No of partitions);
    // The old api
    JavaPairReceiverInputDStream<byte[], byte[]> stream =
      KafkaUtils.createStream(ssc, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class, props, topics, StorageLevel.MEMORY_ONLY_SER()
        );*/

    HashMap<String, String> props = new HashMap<String, String>();
    // Check for null values
    // require only the broker list for direct stream API (low level consumer API)
    if (properties.getProperty("metadataBrokerList") == null) {
      throw new IllegalArgumentException("Property metadata.broker.list cannot be null");
    }
    props.put("metadata.broker.list", properties.getProperty("metadataBrokerList"));
    if (properties.getProperty("topics") == null) {
      throw new IllegalArgumentException("Topic cannot be null");
    }
    String[] topicList = properties.getProperty("topics").split(",");
    LOG.info("Meta data broker list " + properties.getProperty("metadataBrokerList"));
    LOG.info("topic list " + properties.getProperty("topics"));
    JavaPairInputDStream<byte[], byte[]> dStream =
      KafkaUtils.createDirectStream(ssc, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class, props,
        new HashSet<String>(Arrays.asList(topicList)));
    return dStream;
  }

  public void awaitTermination() {
    ssc.awaitTermination();
  }
}
