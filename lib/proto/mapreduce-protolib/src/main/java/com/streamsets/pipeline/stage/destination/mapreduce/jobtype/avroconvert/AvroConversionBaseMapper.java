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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AvroConversionBaseMapper extends Mapper<String, String, NullWritable, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroConversionBaseMapper.class);

  public enum Counters {
    PROCESSED_RECORDS
  }

  // Return true if and only if given property is defined with non empty non default value
  protected boolean propertyDefined(Configuration conf, String propertyName) {
    String prop = conf.get(propertyName);
    // String property will have default empty, integer -1, we'll skip both of them
    return prop != null && !prop.isEmpty() && !prop.equals("-1");
  }

  protected abstract String getTempFilePrefix();
  protected abstract String getOutputFileSuffix();
  protected abstract void closeWriter() throws IOException;
  protected abstract void handleAvroRecord(GenericRecord record) throws IOException;
  protected abstract void initializeWriter(
      Path tempFile,
      Schema avroSchema,
      Configuration conf,
      Context context
  ) throws IOException;

  @Override
  protected void map(String input, String output, Context context) throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Configuration conf = context.getConfiguration();

    LOG.info("Converting input file: {}", input);
    LOG.info("Output directory: {}", output);
    Path inputPath = new Path(input);
    Path outputDir = new Path(output);
    fs.mkdirs(outputDir);

    Path tempFile = new Path(outputDir, getTempFilePrefix() + inputPath.getName());
    if(fs.exists(tempFile)) {
      if(conf.getBoolean(AvroConversionCommonConstants.OVERWRITE_TMP_FILE, false)) {
        fs.delete(tempFile, true);
      } else {
        throw new IOException("Temporary file " + tempFile + " already exists.");
      }
    }
    LOG.info("Using temp file: {}", tempFile);

    // Output file is the same as input except of dropping .avro extension if it exists and appending .parquet or .orc
    String outputFileName = inputPath.getName().replaceAll("\\.avro$", "") + getOutputFileSuffix();
    Path finalFile = new Path(outputDir, outputFileName);
    LOG.info("Final path will be: {}", finalFile);

    // Avro reader
    SeekableInput seekableInput = new FsInput(inputPath, conf);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(seekableInput, reader);
    Schema avroSchema = fileReader.getSchema();

    initializeWriter(tempFile, avroSchema, conf, context);

    LOG.info("Started reading input file");
    long recordCount = 0;
    try {
      while (fileReader.hasNext()) {
        GenericRecord record = fileReader.next();
        handleAvroRecord(record);

        context.getCounter(Counters.PROCESSED_RECORDS).increment(1);
        recordCount++;
      }
    } catch (Exception e) {
      // Various random stuff can happen while converting, so we wrap the underlying exception with more details
      String message = String.format(
          "Exception at offset %d (record %d): %s",
          fileReader.tell(),
          recordCount,
          e.toString()
      );
      throw new IOException(message, e);
    }
    LOG.info("Done reading input file");
    closeWriter();

    LOG.info("Moving temporary file {} to final destination {}", tempFile, finalFile);
    fs.rename(tempFile, finalFile);

    if(!context.getConfiguration().getBoolean(AvroConversionCommonConstants.KEEP_INPUT_FILE, false)) {
      LOG.info("Removing input file", inputPath);
      fs.delete(inputPath, true);
    }

    LOG.info("Done converting input file into output directory {}", output);
  }


}
