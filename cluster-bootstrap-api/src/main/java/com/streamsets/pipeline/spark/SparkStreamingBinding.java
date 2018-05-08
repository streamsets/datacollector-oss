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
package com.streamsets.pipeline.spark;

import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.pipeline.BootstrapCluster;
import com.streamsets.pipeline.ClusterBinding;
import com.streamsets.pipeline.SdcClusterOffsetHelper;
import com.streamsets.pipeline.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

public abstract class SparkStreamingBinding implements ClusterBinding {

  static final String AUTO_OFFSET_RESET = "auto.offset.reset";
  static final String CHECKPOINT_BASE_DIR = ".streamsets-spark-streaming";
  private static final String RDD_CHECKPOINT_DIR = "rdd-checkpoints";
  static final String SDC_ID = "sdc.id";
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingBinding.class);
  private final boolean isRunningInMesos;
  private JavaStreamingContext ssc;
  private final Properties properties;
  private FileSystem hdfs;


  //Static mainly because we can't change the SparkDriverFunction to add this as a private variable instance
  //because of the upgrade from spark checkpointing will fail to deserialize the Spark Driver Function
  public static SdcClusterOffsetHelper offsetHelper;

  //https://issues.streamsets.com/browse/SDC-3961
  //>= 0.9
  private static final String KAFKA_AUTO_RESET_EARLIEST = "earliest";
  private static final String KAFKA_AUTO_RESET_LATEST = "latest";

  //< 0.9
  private static final String KAFKA_AUTO_RESET_SMALLEST = "smallest";
  private static final String KAFKA_AUTO_RESET_LARGEST = "largest";

  private static final Map<String, String> KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES = new HashMap<>();

  static {
    KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES.put(KAFKA_AUTO_RESET_EARLIEST, KAFKA_AUTO_RESET_SMALLEST);
    KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES.put(KAFKA_AUTO_RESET_LATEST, KAFKA_AUTO_RESET_LARGEST);
  }

  public SparkStreamingBinding(Properties properties) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    isRunningInMesos = System.getProperty("SDC_MESOS_BASE_DIR") != null;
  }

  @Override
  public void init() throws Exception {
    for (Object key : properties.keySet()) {
      logMessage("Property => " + key + " => " + properties.getProperty(key.toString()), isRunningInMesos);
    }
    final SparkConf conf = new SparkConf().setAppName("StreamSets Data Collector: " + properties.getProperty(ClusterModeConstants.CLUSTER_PIPELINE_TITLE));
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
    hdfs = (new Path(hdfsURI)).getFileSystem(hadoopConf);

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

    offsetHelper = new SdcClusterOffsetHelper(checkPointPath, hdfs, Utils.getKafkaMaxWaitTime(properties));

    JavaStreamingContextFactory javaStreamingContextFactory = getStreamingContextFactory(
        conf,
        topic,
        consumerGroup,
        Utils.getPropertyOrEmptyString(properties, AUTO_OFFSET_RESET),
        isRunningInMesos
    );

    ssc = javaStreamingContextFactory.create();
    Path rddCheckpointDir = new Path(checkPointPath, RDD_CHECKPOINT_DIR);
    if (hdfs.exists(rddCheckpointDir)) {
      hdfs.delete(rddCheckpointDir, true);
    }
    hdfs.mkdirs(rddCheckpointDir);
    ssc.checkpoint(rddCheckpointDir.toString());

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
  }

  protected Properties getProperties() {
    return properties;
  }

  protected String getTopic() {
    return Utils.getKafkaTopic(getProperties());
  }

  protected String getConsumerGroup() {
    return Utils.getKafkaConsumerGroup(getProperties());
  }

  //Visible For Testing (TODO - Import guava and make sure it doesn't conflict with spark's rootclassloader)
  CheckpointPath getCheckPointPath(String topic, String consumerGroup) {
    return new CheckpointPath.Builder(CHECKPOINT_BASE_DIR)
        .sdcId(Utils.getPropertyNotNull(properties, SDC_ID))
        .topic(topic)
        .consumerGroup(consumerGroup)
        .pipelineName(Utils.getPropertyNotNull(properties, ClusterModeConstants.CLUSTER_PIPELINE_NAME))
        .build();
  }

  static String getConfigurableAutoOffsetResetIfNonEmpty(String autoOffsetValue) {
    String configurableAutoOffsetResetValue = KAFKA_POST_0_9_TO_PRE_0_9_CONFIG_CHANGES.get(autoOffsetValue);
    return (configurableAutoOffsetResetValue == null)? autoOffsetValue : configurableAutoOffsetResetValue;
  }

  public JavaStreamingContext getStreamingContext() {
    return ssc;
  }

  public void startContext() {
    ssc.start();
  }

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
  public void awaitTermination() throws Exception {
    ssc.awaitTermination();
  }

  @Override
  public void close() {
    if (ssc != null) {
      ssc.close();
    }
    if (hdfs != null) {
      try {
        hdfs.close();
      } catch (IOException e) {
        LOG.error("Failed to close FileSystem. Reason: {}", e.toString(), e);
      }
    }
  }

  static void logMessage(String message, boolean isRunningInMesos) {
    if (isRunningInMesos) {
      System.out.println(message);
    } else {
      LOG.info(message);
    }
  }

  public abstract JavaStreamingContextFactory getStreamingContextFactory(
      SparkConf conf,
      String topic,
      String groupId,
      String autoOffsetValue,
      boolean isRunningInMesos
  );

  public abstract class JavaStreamingContextFactoryImpl implements JavaStreamingContextFactory {

    final SparkConf sparkConf;
    final long duration;
    final String metaDataBrokerList;
    final String topic;
    final int numberOfPartitions;
    final String groupId;
    final boolean isRunningInMesos;
    final int maxRatePerPartition;
    final Map<String, String> extraKafkaConfigs;
    String autoOffsetValue;

    public JavaStreamingContextFactoryImpl (
        SparkConf sparkConf,
        long duration,
        String metaDataBrokerList,
        String topic,
        int numberOfPartitions,
        String groupId,
        String autoOffsetValue,
        boolean isRunningInMesos,
        int maxRatePerPartition,
        Map<String, String> extraKafkaConfigs
    ) {
      this.sparkConf = sparkConf;
      this.duration = duration;
      this.metaDataBrokerList = metaDataBrokerList;
      this.topic = topic;
      this.numberOfPartitions = numberOfPartitions;
      this.autoOffsetValue = autoOffsetValue;
      this.isRunningInMesos = isRunningInMesos;
      this.groupId = groupId;
      this.maxRatePerPartition = maxRatePerPartition;
      this.extraKafkaConfigs = extraKafkaConfigs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public JavaStreamingContext create() {
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", String.valueOf(maxRatePerPartition));
      // Use our classpath first, since we ship a newer version of Jackson and possibly other deps in the future.
      sparkConf.set("spark.driver.userClassPathFirst", "true");
      sparkConf.set("spark.executor.userClassPathFirst", "true");

      JavaStreamingContext result = new JavaStreamingContext(sparkConf, new Duration(duration));
      Map<String, Object> props = new HashMap<>();

      props.put("group.id", groupId);
      props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      for (Map.Entry<String, Object> map : props.entrySet()) {
        logMessage(Utils.format("Adding extra kafka config, {}:{}", map.getKey(), map.getValue()), isRunningInMesos);
      }

      logMessage("Meta data broker list " + metaDataBrokerList, isRunningInMesos);
      logMessage("Topic is " + topic, isRunningInMesos);
      logMessage("Auto offset reset is set to " + autoOffsetValue, isRunningInMesos);
      return createDStream(result, props);
    }

    public abstract JavaStreamingContext createDStream(JavaStreamingContext result, Map<String, Object> props);
  }
}
