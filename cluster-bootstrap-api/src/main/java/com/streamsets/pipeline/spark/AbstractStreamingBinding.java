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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.Properties;

public abstract class AbstractStreamingBinding implements ClusterBinding {
  static final String AUTO_OFFSET_RESET = "auto.offset.reset";
  static final String CHECKPOINT_BASE_DIR = ".streamsets-spark-streaming";
  static final String SDC_ID = "sdc.id";
  static final String CLUSTER_PIPELINE_NAME = "cluster.pipeline.name";
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamingBinding.class);
  private final boolean isRunningInMesos;
  private JavaStreamingContext ssc;
  private final Properties properties;

  public AbstractStreamingBinding(Properties properties) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    isRunningInMesos = System.getProperty("SDC_MESOS_BASE_DIR") != null ? true: false;
  }

  protected  Properties getProperties() {
    return properties;
  }

  protected abstract String getTopic();

  protected abstract String getConsumerGroup();

  //Visible For Testing (TODO - Import guava and make sure it doesn't conflict with spark's rootclassloader)
  CheckpointPath getCheckPointPath(String topic, String consumerGroup) {
    CheckpointPath sdcCheckpointPath = new CheckpointPath.Builder(CHECKPOINT_BASE_DIR)
        .sdcId(Utils.getPropertyNotNull(properties, SDC_ID))
        .topic(topic)
        .consumerGroup(consumerGroup)
        .pipelineName(Utils.getPropertyNotNull(properties, CLUSTER_PIPELINE_NAME))
        .build();
    return sdcCheckpointPath;
  }

  @Override
  public void init() throws Exception {
    for (Object key : properties.keySet()) {
      logMessage("Property => " + key + " => " + properties.getProperty(key.toString()), isRunningInMesos);
    }
    final SparkConf conf = new SparkConf().setAppName("StreamSets Data Collector - Streaming Mode");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    final String topic = getTopic();
    final String consumerGroup = getConsumerGroup();
    Configuration hadoopConf = new SparkHadoopUtil().newConfiguration(conf);
    if (isRunningInMesos) {
      hadoopConf = getHadoopConf(hadoopConf);
    } else {
      hadoopConf = new Configuration();
    }
    URI hdfsURI = FileSystem.getDefaultUri(hadoopConf);
    logMessage("Default FS URI: " + hdfsURI, isRunningInMesos);
    FileSystem hdfs = (new Path(hdfsURI)).getFileSystem(hadoopConf);


    final Path checkPointPath = new Path(
        hdfs.getHomeDirectory(),
        getCheckPointPath(topic, consumerGroup).getPath()
    );
    hdfs.mkdirs(checkPointPath);
    if (!hdfs.isDirectory(checkPointPath)) {
      throw new IllegalStateException("Could not create checkpoint path: " + checkPointPath);
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
        conf.setJars(new String[] { Utils.getPropertyNotNull(properties, "mesos.jar.url") });
      } else {
        throw new IllegalStateException("Unsupported scheme: " + scheme);
      }
    }

    JavaStreamingContextFactory javaStreamingContextFactory = getStreamingContextFactory(
        conf,
        checkPointPath.toString(),
        topic,
        consumerGroup,
        Utils.getPropertyOrEmptyString(properties, AUTO_OFFSET_RESET),
        isRunningInMesos
    );

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

  protected abstract JavaStreamingContextFactory getStreamingContextFactory(
      SparkConf conf,
      String checkPointPath,
      String topic,
      String groupId,
      String autoOffsetValue,
      boolean isRunningInMesos
  );

  private Configuration getHadoopConf(Configuration conf) {
    String hdfsS3ConfProp = Utils.getPropertyOrEmptyString(properties, "hdfsS3ConfDir");
    if (!hdfsS3ConfProp.isEmpty()) {
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
    } else {
      throw new IllegalArgumentException("Cannot find hdfs/S3 config; hdfsS3ConfDir cannot be empty");
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

  static void logMessage(String message, boolean isRunningInMesos) {
    if (isRunningInMesos) {
      System.out.println(message);
    } else {
      LOG.info(message);
    }
  }

}
