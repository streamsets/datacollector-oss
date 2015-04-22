/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;
import org.apache.spark.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;

import java.util.Properties;

public class SparkStreamingBinding {
  public static final String INPUT_TYPE = "streamsets.cluster.input.type";
  public static final String HDFS_INPUT_TYPE = "hdfs-dir";

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
    JavaDStream<String> dStream;
    String inputType = properties.getProperty(INPUT_TYPE);
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
    ssc.start();
  }

  public void awaitTermination() {
    ssc.awaitTermination();
  }
}
