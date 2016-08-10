/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Mapper that takes input as file path to avro file and converts it into parquet file - all from within this one map task.
 */
public class AvroParquetConvertMapper extends Mapper<String, String, NullWritable, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroParquetConvertMapper.class);

  public enum Counters {
    PROCESSED_RECORDS
  }

  @Override
  protected void map(String input, String output, Context context) throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(context.getConfiguration());

    LOG.info("Converting input file: {}", input);
    LOG.info("Output directory: {}", output);
    Path inputPath = new Path(input);
    Path outputDir = new Path(output);
    fs.mkdirs(outputDir);

    Path tempFile = new Path(outputDir, ".tmp_" + inputPath.getName());
    if(fs.exists(tempFile)) {
      throw new IOException("Temporary file " + tempFile + " already exists.");
    }
    LOG.info("Using temp file: {}", tempFile);

    // Output file is the same as input except of dropping .avro extension if it exists and appending .parquet
    String outputFileName = inputPath.getName().replaceAll("\\.avro$", "") + ".parquet";
    Path finalFile = new Path(outputDir, outputFileName);
    LOG.info("Final path will be: {}", finalFile);

    // Avro reader
    SeekableInput seekableInput = new FsInput(inputPath, context.getConfiguration());
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(seekableInput, reader);
    Schema avroSchema = fileReader.getSchema() ;

    // Parquet writer
    ParquetWriter parquetWriter = AvroParquetWriter.builder(tempFile)
      .withSchema(avroSchema)
      .withConf(context.getConfiguration())
      .build();

    LOG.info("Started reading input file");
    while (fileReader.hasNext()) {
      GenericRecord record = fileReader.next();
      parquetWriter.write(record);

      context.getCounter(Counters.PROCESSED_RECORDS).increment(1);
    }
    LOG.info("Done reading input file");
    parquetWriter.close();

    LOG.info("Moving temporary file {} to final destination {}", tempFile, finalFile);
    fs.rename(tempFile, finalFile);

    if(!context.getConfiguration().getBoolean(AvroParquetConstants.KEEP_INPUT_FILE, false)) {
      LOG.info("Removing input file", inputPath);
      fs.delete(inputPath, true);
    }

    LOG.info("Done converting input file {}", output);
  }
}
