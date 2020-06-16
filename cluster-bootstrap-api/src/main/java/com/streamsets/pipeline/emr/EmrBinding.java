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
package com.streamsets.pipeline.emr;

import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.pipeline.ClusterBinding;
import com.streamsets.pipeline.Utils;
import com.streamsets.pipeline.hadoop.HadoopMapReduceBinding;
import com.streamsets.pipeline.hadoop.PipelineMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class EmrBinding implements ClusterBinding {

  private static final Logger LOG = LoggerFactory.getLogger(EmrBinding.class);
  private final String[] args;
  private Properties properties;
  private Job job;
  private static final String TEXTINPUTFORMAT_RECORD_DELIMITER = "textinputformat.record.delimiter" ;
  private static final List<String> overriddenConfs = Arrays.asList(TEXTINPUTFORMAT_RECORD_DELIMITER,
      FileInputFormat.INPUT_DIR,
      FileInputFormat.INPUT_DIR_RECURSIVE,
      CommonConfigurationKeys.FS_DEFAULT_NAME_KEY
  );


  // args[0] is archives,
  // args[1] is libjars,
  // args[2] is unique prefix,
  // args[3] is java opts
  // args[4] is log level
  public EmrBinding(String[] args) {
    this.args = args;
  }

  @Override
  public void init() throws Exception {
    Configuration conf = new Configuration();
    LOG.info("Arg 0: {}, Arg 1: {}, Arg 2: {}, Arg 3: {}, Arg 4: {}", args[0], args[1], args[2], args[3], args[4]);
    try (InputStream in = getClass().getClassLoader().getResourceAsStream("cluster_sdc.properties")) {
      properties = new Properties();
      properties.load(in);
      String dataFormat = Utils.getHdfsDataFormat(properties);
      for (Object key : properties.keySet()) {
        String realKey = String.valueOf(key);
        // TODO - Override other configs set in HdfsSource
        if (overriddenConfs.contains(realKey)) {
          String value = Utils.getPropertyNotNull(properties, realKey);
          conf.set(realKey, value);
        }
      }
      String javaOpts = args[3];
      Integer mapMemoryMb = HadoopMapReduceBinding.getMapMemoryMb(javaOpts, conf);
      if (mapMemoryMb != null) {
        conf.set(HadoopMapReduceBinding.MAPREDUCE_MAP_MEMORY_MB, String.valueOf(mapMemoryMb));
      }
      conf.set(HadoopMapReduceBinding.MAPREDUCE_JAVA_OPTS, javaOpts);

      conf.setBoolean("mapreduce.map.speculative", false);
      conf.setBoolean("mapreduce.reduce.speculative", false);
      if ("AVRO".equalsIgnoreCase(dataFormat)) {
        conf.set(Job.INPUT_FORMAT_CLASS_ATTR, "org.apache.avro.mapreduce.AvroKeyInputFormat");
        conf.set(Job.MAP_OUTPUT_KEY_CLASS, "org.apache.avro.mapred.AvroKey");
      }

      conf.set(MRJobConfig.MAP_LOG_LEVEL, args[4]);
      job = Job.getInstance(
          conf,
          "StreamSets Data Collector: " + properties.getProperty(ClusterModeConstants.CLUSTER_PIPELINE_TITLE) + "::" + args[2]
      );
      for (String archive : Arrays.asList(args[0].split("\\s*,\\s*"))) {
        job.addCacheArchive(new URI(archive));
      }
      for (String libJar : Arrays.asList(args[1].split("\\s*,\\s*"))) {
        job.addFileToClassPath(new Path(libJar));
      }
      job.setJarByClass(this.getClass());
      job.setNumReduceTasks(0);
      if (!"AVRO".equalsIgnoreCase(dataFormat)) {
        job.setOutputKeyClass(NullWritable.class);
      }
      job.setMapperClass(PipelineMapper.class);

      job.setOutputValueClass(NullWritable.class);

      job.setOutputFormatClass(NullOutputFormat.class);
    }

  }

  @Override
  public void awaitTermination() throws Exception {
    job.waitForCompletion(true);
  }

  @Override
  public void close() throws Exception {
    if (job != null) {
      job.killJob();
    }
  }
}
