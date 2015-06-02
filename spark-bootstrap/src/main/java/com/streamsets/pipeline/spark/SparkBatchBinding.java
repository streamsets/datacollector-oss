/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.spark;
import com.streamsets.pipeline.ClusterBinding;
import com.streamsets.pipeline.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SparkBatchBinding implements ClusterBinding {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchBinding.class);

  private JavaSparkContext ssc;
  private Properties properties;
  private String pipelineJson;

  public SparkBatchBinding(Properties properties, String pipelineJson) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    this.pipelineJson = Utils.checkNotNull(pipelineJson, "Pipeline JSON");
  }

  @Override
  public void init() throws Exception {
    SparkConf conf = new SparkConf().setAppName("StreamSets Data Collector - Batch Mode");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    ssc = new JavaSparkContext(conf);
    String hdfsDirLocation = getProperty("hdfsDirLocation");
    Configuration hadoopConf = new Configuration();
    hadoopConf.set(FileInputFormat.INPUT_DIR_RECURSIVE, getProperty(FileInputFormat.INPUT_DIR_RECURSIVE));
    hadoopConf.set(FileInputFormat.SPLIT_MAXSIZE, getProperty(FileInputFormat.SPLIT_MAXSIZE));
    hadoopConf.set("mapreduce.input.fileinputformat.list-status.num-threads",
      getProperty("mapreduce.input.fileinputformat.list-status.num-threads"));
    LOG.info("Creating RDD for file path " + hdfsDirLocation);
    JavaPairRDD<LongWritable, Text> javaRdd =
      ssc.newAPIHadoopFile(hdfsDirLocation, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf);

    final Thread shutdownHookThread = new Thread("Spark.shutdownHook") {
      @Override
      public void run() {
        LOG.debug("Gracefully stopping Spark Application");
        ssc.stop();
        LOG.info("Application stopped");
      }
    };
    Runtime.getRuntime().addShutdownHook(shutdownHookThread);
    LOG.info("Making calls through spark context ");
    javaRdd.foreachPartition(new BootstrapSparkFunction(properties, pipelineJson));
  }

  private String getProperty(String name) {
    Utils.checkArgumentNotNull(properties.getProperty(name),
      "Property " + name +" cannot be null");
    LOG.info("Value of property: " + name + " is " + properties.getProperty(name).trim());
    return properties.getProperty(name).trim();
  }

  @Override
  public void awaitTermination() {
  }

  @Override
  public void close() {
    if (ssc != null) {
      ssc.close();
    }
  }
}
