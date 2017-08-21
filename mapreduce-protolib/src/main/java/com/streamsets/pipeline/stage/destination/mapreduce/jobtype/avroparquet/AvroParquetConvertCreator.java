/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet;

import com.streamsets.pipeline.stage.destination.mapreduce.MapreduceUtils;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.ParquetWriter;

import java.util.concurrent.Callable;

public class AvroParquetConvertCreator implements Configurable, Callable<Job> {

  Configuration conf;

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Job call() throws Exception {
    // We're explicitly disabling speculative execution
    conf.set("mapreduce.map.speculative", "false");
    conf.set("mapreduce.map.maxattempts", "1");
    MapreduceUtils.addJarsToJob(conf,
      SemanticVersion.class,
      ParquetWriter.class,
      AvroParquetWriter.class,
      FsInput.class,
      CompressionCodec.class,
      ParquetProperties.class,
      BytesInput.class
    );

    Job job = Job.getInstance(conf);

    // IO formats
    job.setInputFormatClass(AvroParquetInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    // Mapper & job output
    job.setMapperClass(AvroParquetConvertMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    // It's map only job
    job.setNumReduceTasks(0);

    // General configuration
    job.setJarByClass(getClass());

    return job;
  }
}
