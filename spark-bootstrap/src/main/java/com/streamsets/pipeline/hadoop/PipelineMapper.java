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
package com.streamsets.pipeline.hadoop;

import com.streamsets.pipeline.BootstrapCluster;
import com.streamsets.pipeline.impl.ClusterFunction;
import com.streamsets.pipeline.impl.Pair;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PipelineMapper extends Mapper {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineMapper.class);
  private static final String CLUSTER_HDFS_CONFIG_BEAN_PREFIX = "clusterHDFSConfigBean.";

  @Override
  protected void setup(Mapper.Context context) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void map(Object key, Object value, Mapper.Context context)
    throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void run(Mapper.Context context) throws IOException, InterruptedException {
    Integer id = context.getTaskAttemptID().getTaskID().getId();
    Properties properties;
    ClusterFunction clusterFunction;
    try {
      properties = BootstrapCluster.getProperties();
      clusterFunction = (ClusterFunction)BootstrapCluster.getClusterFunction(id);
    } catch (Exception ex) {
      if (ex instanceof RuntimeException) {
        throw (RuntimeException)ex;
      } else if (ex instanceof IOException) {
        throw (IOException)ex;
      } else if (ex instanceof InterruptedException) {
        throw (InterruptedException)ex;
      } else {
        throw new RuntimeException("Error initializing: " + ex, ex);
      }
    }

    InputSplit inputSplit = context.getInputSplit();
    FileSplit fileSplit = null;
    String file = "unknown::";
    if (inputSplit instanceof FileSplit) {
      fileSplit = (FileSplit)inputSplit;
    } else {
      throw new IllegalStateException("Unsupported InputSplit: " + inputSplit.getClass().getName());
    }
    String header = null;
    if (fileSplit != null) {
      file = fileSplit.getPath() + "::" + fileSplit.getStart();
      if (properties.getProperty(CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "dataFormat").equals("DELIMITED")
        && properties.getProperty(CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "dataFormatConfig.csvHeader").equals("WITH_HEADER")) {
        if (fileSplit.getStart() == 0) {
          boolean hasNext = context.nextKeyValue();
          if (hasNext) {
            header = String.valueOf(context.getCurrentValue());
          }
        } else {
          header = PipelineMapper.getHeaderFromFile(context.getConfiguration(), fileSplit.getPath());
        }
        LOG.info("Header in file " + fileSplit.getPath() + " for start offset " + fileSplit.getStart() + ": " + header);
      }
    }
    boolean isAvro = properties.getProperty(CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "dataFormat").equalsIgnoreCase("AVRO");
    int batchSize = Integer.parseInt(properties.getProperty(CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "maxBatchSize", "1000").trim());
    boolean errorOccurred = true;
    try {
      boolean hasNext = context.nextKeyValue();
      int count = 0;
      while (hasNext) {
        List<Map.Entry> batch = new ArrayList<>();
        if (header != null) {
          // we pass the header each time because the CSV parser operates
          // on each record as if it were a file in batch mode
          batch.add(new Pair(header, null));
          // increment batch size as adding the first entry as header
          batchSize = batchSize + 1;
        }
        while (hasNext && batch.size() < batchSize) {
          if (isAvro) {
            GenericRecord avroMessageWrapper = ((AvroKey<GenericRecord>) context.getCurrentKey()).datum();
            batch.add(new Pair(file + "::" + count, PipelineMapper.getBytesFromAvroRecord(avroMessageWrapper)));
            count++;
          } else {
            batch.add(new Pair(file + context.getCurrentKey(), String.valueOf(context.getCurrentValue())));
          }
          hasNext = context.nextKeyValue(); // not like iterator.hasNext, actually advances
        }
        clusterFunction.invoke(batch);
      }
      errorOccurred = false;
    } catch (Exception ex) {
      String msg = "Error invoking map function: " + ex;
      LOG.error(msg, ex);
      throw new RuntimeException(msg, ex);
    } finally {
      try {
        clusterFunction.shutdown();
      } catch (Throwable throwable) {
        LOG.warn("Error on destroy: {}", throwable, throwable);
        if (!errorOccurred) {
          if (throwable instanceof RuntimeException) {
            throw (RuntimeException)throwable;
          } else if (throwable instanceof Error) {
            throw (Error)throwable;
          } else {
            throw new RuntimeException(throwable);
          }
        }
      }
    }
  }

  private static byte[] getBytesFromAvroRecord(GenericRecord genericRecord) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
    dataFileWriter.create(genericRecord.getSchema(), out);
    dataFileWriter.append(genericRecord);
    dataFileWriter.close();
    return out.toByteArray();
  }

  private static String getHeaderFromFile(Configuration hadoopConf, Path path) throws IOException {
    String header;
    BufferedReader br = null;
    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      br = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8));
      // read one line - the header
      header = br.readLine();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          LOG.warn("Error while closing file: '{}', exception string is: '{}'", path, e, e);
          br = null;
        }
      }
    }
    return header;
  }
}
