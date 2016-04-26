/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.BootstrapCluster;
import com.streamsets.pipeline.ClusterBinding;
import com.streamsets.pipeline.Utils;

import kafka.serializer.DefaultDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

public class SparkStreamingBinding implements ClusterBinding {
  private static final String MAX_WAIT_TIME = "kafkaConfigBean.maxWaitTime";
  private static final String METADATA_BROKER_LIST = "kafkaConfigBean.metadataBrokerList";
  private static final String TOPIC = "kafkaConfigBean.topic";
  private static final String AUTO_OFFSET_RESET = "auto.offset.reset";
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingBinding.class);
  private final boolean isRunningInMesos;
  private JavaStreamingContext ssc;
  private final Properties properties;

  public SparkStreamingBinding(Properties properties) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    isRunningInMesos = System.getProperty("SDC_MESOS_BASE_DIR") != null ? true: false;
  }

  @Override
  public void init() throws Exception {
    for (Object key : properties.keySet()) {
      logMessage("Property => " + key + " => " + properties.getProperty(key.toString()), isRunningInMesos);
    }
    final SparkConf conf = new SparkConf().setAppName("StreamSets Data Collector - Streaming Mode");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    final String topic = getProperty(TOPIC);
    final long duration;
    String durationAsString = getProperty(MAX_WAIT_TIME);
    try {
      duration = Long.parseLong(durationAsString);
    } catch (NumberFormatException ex) {
      String msg = "Invalid " + MAX_WAIT_TIME  + " '" + durationAsString + "' : " + ex;
      throw new IllegalArgumentException(msg, ex);
    }

    Configuration hadoopConf = new SparkHadoopUtil().newConfiguration(conf);
    if (isRunningInMesos) {
      hadoopConf = getHadoopConf(hadoopConf);
    } else {
      hadoopConf = new Configuration();
    }
    URI hdfsURI = FileSystem.getDefaultUri(hadoopConf);
    logMessage("Default FS URI: " + hdfsURI, isRunningInMesos);
    FileSystem hdfs = (new Path(hdfsURI)).getFileSystem(hadoopConf);
    Path sdcCheckpointPath = new Path(hdfs.getHomeDirectory(), ".streamsets-spark-streaming/"
      + getProperty("sdc.id") + "/" + encode(topic));
    // encode as remote pipeline name might have colon within it
    String pipelineName = encode(getProperty("cluster.pipeline.name"));
    final Path checkPointPath = new Path(sdcCheckpointPath, pipelineName);
    hdfs.mkdirs(checkPointPath);
    if (!hdfs.isDirectory(checkPointPath)) {
      throw new IllegalStateException("Could not create checkpoint path: " + sdcCheckpointPath);
    }
    if (isRunningInMesos) {
      String scheme = hdfsURI.getScheme();
      if (scheme.equals("hdfs")) {
        File mesosBootstrapFile = BootstrapCluster.getMesosBootstrapFile();
        Path mesosBootstrapPath = new Path(checkPointPath, mesosBootstrapFile.getName());
        // in case of hdfs, copy the jar file from local path to hdfs
        hdfs.copyFromLocalFile(false, true, new Path(mesosBootstrapFile.toURI()), mesosBootstrapPath);
        conf.setJars(new String[] { mesosBootstrapPath.toString() });
      } else if (scheme.equals("s3") || scheme.equals("s3n") || scheme.equals("s3a")) {
        // we cant upload the jar to s3 as executors wont understand s3 scheme without the aws jar.
        // So have the jar available on http
        conf.setJars(new String[] { getProperty("mesos.jar.url") });
      } else {
        throw new IllegalStateException("Unsupported scheme: " + scheme);
      }
    }
    JavaStreamingContextFactory javaStreamingContextFactory = new JavaStreamingContextFactoryImpl(conf, duration, checkPointPath.toString(),
      getProperty(METADATA_BROKER_LIST), topic, properties.getProperty(AUTO_OFFSET_RESET, "").trim(), isRunningInMesos);

    ssc = JavaStreamingContext.getOrCreate(checkPointPath.toString(), hadoopConf, javaStreamingContextFactory, true);
    // mesos tries to stop the context internally, so don't do it here - deadlock bug in spark
    if (!isRunningInMesos) {
      final Thread shutdownHookThread = new Thread("Spark.shutdownHook") {
        @Override
        public void run() {
          LOG.debug("Gracefully stopping Spark Streaming Application");
          ssc.stop(true, true);
          LOG.info("Application stopped");
        }
      };
      Runtime.getRuntime().addShutdownHook(shutdownHookThread);
    }
    logMessage("Making calls through spark context ", isRunningInMesos);
    ssc.start();
  }

  private static class JavaStreamingContextFactoryImpl implements JavaStreamingContextFactory {

    private final SparkConf sparkConf;
    private final long duration;
    private final String checkPointPath;
    private final String metaDataBrokerList;
    private final String topic;
    private final boolean isRunningInMesos;
    private final String autoOffsetValue;

    public JavaStreamingContextFactoryImpl(SparkConf sparkConf, long duration, String checkPointPath,
      String metaDataBrokerList, String topic, String autoOffsetValue, boolean isRunningInMesos) {
      this.sparkConf = sparkConf;
      this.duration = duration;
      this.checkPointPath = checkPointPath;
      this.metaDataBrokerList = metaDataBrokerList;
      this.topic = topic;
      this.autoOffsetValue = autoOffsetValue;
      this.isRunningInMesos = isRunningInMesos;
    }

    @Override
    public JavaStreamingContext create() {
      JavaStreamingContext result = new JavaStreamingContext(sparkConf, new Duration(duration));
      result.checkpoint(checkPointPath);
      Map<String, String> props = new HashMap<String, String>();
      props.put("metadata.broker.list", metaDataBrokerList);
      if (!autoOffsetValue.isEmpty()) {
        props.put(AUTO_OFFSET_RESET, autoOffsetValue);
      }
      logMessage("Meta data broker list " + metaDataBrokerList, isRunningInMesos);
      logMessage("topic list " + topic, isRunningInMesos);
      logMessage("Auto offset is set to " + autoOffsetValue, isRunningInMesos);
      JavaPairInputDStream<byte[], byte[]> dStream =
        KafkaUtils.createDirectStream(result, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class,
          props, new HashSet<String>(Arrays.asList(topic.split(","))));
      dStream.foreachRDD(new SparkDriverFunction());
      return result;
    }
  }

  static String encode(String s) {
    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Could not find UTF-8: " + e, e);
    }
  }

  private String getProperty(String name) {
    Utils.checkArgumentNotNull(properties.getProperty(name),
      "Property " + name +" cannot be null");
    return properties.getProperty(name).trim();
  }

  private Configuration getHadoopConf(Configuration conf) {
    String hdfsS3ConfProp = properties.getProperty("hdfsS3ConfDir");
    if (hdfsS3ConfProp != null && !hdfsS3ConfProp.isEmpty()) {
      File hdfsS3ConfDir = new File(System.getProperty("sdc.resources.dir"), hdfsS3ConfProp).getAbsoluteFile();
      if (!hdfsS3ConfDir.exists()) {
        throw new IllegalArgumentException("The config dir for hdfs/S3 doesn't exist");
      } else {
        File coreSite = new File(hdfsS3ConfDir, "core-site.xml");
        if (coreSite.exists()) {
          conf.addResource(new Path(coreSite.getAbsolutePath()));
        } else {
          throw new IllegalStateException(
            "Core-site xml for configuring Hadoop/S3 filesystem is required for checkpoint related metadata while running Spark Streaming");
        }
        File hdfsSite = new File(hdfsS3ConfDir, "hdfs-site.xml");
        if (hdfsSite.exists()) {
          conf.addResource(new Path(hdfsSite.getAbsolutePath()));
        }
      }
    }
    if ((hdfsS3ConfProp == null || hdfsS3ConfProp.isEmpty())) {
      throw new IllegalArgumentException(
        "Cannot find hdfs/S3 config; hdfsS3ConfDir cannot be null");
    }
    return conf;
  }

  @Override
  public void awaitTermination() {
    ssc.awaitTermination();
  }

  @Override
  public void close() {
    if (ssc != null) {
      ssc.close();
    }
  }

  private static void logMessage(String message, boolean isRunningInMesos) {
    if (isRunningInMesos) {
      System.out.println(message);
    } else {
      LOG.info(message);
    }
  }

}
