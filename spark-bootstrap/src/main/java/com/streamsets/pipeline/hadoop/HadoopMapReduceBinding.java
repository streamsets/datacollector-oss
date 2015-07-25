/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hadoop;

import com.streamsets.pipeline.ClusterBinding;
import com.streamsets.pipeline.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HadoopMapReduceBinding implements ClusterBinding {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopMapReduceBinding.class);
  private final String[] args;
  private Properties properties;
  private Job job;

  public HadoopMapReduceBinding(String[] args) {
    this.args = args;
  }

  private String getProperty(String name) {
    Utils.checkArgumentNotNull(properties.getProperty(name),
      "Property " + name + " cannot be null");
    LOG.info("Value of property: " + name + " is " + properties.getProperty(name).trim());
    return properties.getProperty(name).trim();
  }

  @Override
  public void init() throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = parser.getRemainingArgs();
    properties = new Properties();
    if (remainingArgs.length != 2) {
      List<String> argsList = new ArrayList<>();
      for (String arg : remainingArgs) {
        argsList.add("'" + arg + "'");
      }
      throw new IllegalArgumentException("Error expected properties-file java-opts got: " + argsList);
    }
    String propertiesFile = remainingArgs[0];
    String javaOpts = remainingArgs[1];
    try (InputStream in = new FileInputStream(propertiesFile)) {
      properties.load(in);
      String hdfsUri = getProperty("hdfsUri");
      String source = this.getClass().getSimpleName();
      for (Object key : properties.keySet()) {
        String realKey = String.valueOf(key);
        String value = properties.getProperty(realKey);
        conf.set(realKey, value, source);
      }
      conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, getProperty(FileInputFormat.INPUT_DIR_RECURSIVE));
      conf.set(FileInputFormat.SPLIT_MAXSIZE, getProperty(FileInputFormat.SPLIT_MAXSIZE));
      conf.set("mapreduce.input.fileinputformat.list-status.num-threads",
        getProperty("mapreduce.input.fileinputformat.list-status.num-threads"));
      conf.set("mapred.child.java.opts", javaOpts);
      conf.setBoolean("mapreduce.map.speculative", false);
      conf.setBoolean("mapreduce.reduce.speculative", false);
      job = Job.getInstance(conf, "StreamSets Data Collector - Batch Mode");
      job.setJarByClass(this.getClass());
      job.setNumReduceTasks(0);
      job.setMapperClass(PipelineMapper.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      Path fsRoot = new Path(hdfsUri);
      for (String hdfsDirLocation : getProperty("hdfsDirLocations").split(",")) {
        Path path = new Path(fsRoot, hdfsDirLocation);
        FileInputFormat.addInputPath(job, path);
        LOG.info("Input path: " + path);
      }
    }
  }

  @Override
  public void awaitTermination() throws Exception {
    job.waitForCompletion(true); // killed by ClusterProviderImpl before returning
  }

  @Override
  public void close() throws Exception {
    job.killJob();
  }
}
