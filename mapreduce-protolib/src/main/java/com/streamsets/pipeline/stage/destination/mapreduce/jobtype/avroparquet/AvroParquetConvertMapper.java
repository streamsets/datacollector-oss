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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.Version;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverterLogicalTypesPre19;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Mapper that takes input as file path to avro file and converts it into parquet file - all from within this one map task.
 */
public class AvroParquetConvertMapper extends Mapper<String, String, NullWritable, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroParquetConvertMapper.class);

  /**
   * Custom Builder to inject our own writer support that can work with logical types in older parquet version(s). The
   * logic is functionally equivalent to AvroParquetWriter.Builder
   */
  public static class Builder<T> extends org.apache.parquet.hadoop.ParquetWriter.Builder<T, Builder<T>> {
    private Schema schema;
    private GenericData model;

    private Builder(Path file) {
      super(file);
      this.schema = null;
      this.model = SpecificData.get();
    }

    public Builder<T> withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder<T> withDataModel(GenericData model) {
      this.model = model;
      return this;
    }

    protected Builder<T> self() {
      return this;
    }

    protected WriteSupport<T> getWriteSupport(Configuration conf) {
      return new AvroWriteSupport((new AvroSchemaConverterLogicalTypesPre19(conf)).convert(this.schema), this.schema, this.model);
    }
  }

  public enum Counters {
    PROCESSED_RECORDS
  }

  // Return true if and only if given property is defined with non empty non default value
  private boolean propertyDefined(Configuration conf, String propertyName) {
    String prop = conf.get(propertyName);
    // String property will have default empty, integer -1, we'll skip both of them
    return prop != null && !prop.isEmpty() && !prop.equals("-1");
  }

  @Override
  protected void map(String input, String output, Context context) throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Configuration conf = context.getConfiguration();

    LOG.info("Converting input file: {}", input);
    LOG.info("Output directory: {}", output);
    Path inputPath = new Path(input);
    Path outputDir = new Path(output);
    fs.mkdirs(outputDir);

    Path tempFile = new Path(outputDir, AvroParquetConstants.TMP_PREFIX + inputPath.getName());
    if(fs.exists(tempFile)) {
      if(conf.getBoolean(AvroParquetConstants.OVERWRITE_TMP_FILE, false)) {
        fs.delete(tempFile, true);
      } else {
        throw new IOException("Temporary file " + tempFile + " already exists.");
      }
    }
    LOG.info("Using temp file: {}", tempFile);

    // Output file is the same as input except of dropping .avro extension if it exists and appending .parquet
    String outputFileName = inputPath.getName().replaceAll("\\.avro$", "") + ".parquet";
    Path finalFile = new Path(outputDir, outputFileName);
    LOG.info("Final path will be: {}", finalFile);

    // Avro reader
    SeekableInput seekableInput = new FsInput(inputPath, conf);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(seekableInput, reader);
    Schema avroSchema = fileReader.getSchema() ;

    // Detect Parquet version to see if it supports logical types
    LOG.info("Detected Parquet version: " + Version.FULL_VERSION);

    // Parquet Avro pre-1.9 doesn't work with logical types, so in that case we use custom Builder that injects our own
    // avro schema -> parquet schema generator class (which is a copy of the one that was provided in PARQUET-358).
    ParquetWriter.Builder builder = null;
    try {
      SemanticVersion parquetVersion = SemanticVersion.parse(Version.VERSION_NUMBER);
      if(parquetVersion.major > 1 || (parquetVersion.major == 1 && parquetVersion.minor >= 9)) {
        builder = AvroParquetWriter.builder(tempFile).withSchema(avroSchema);
      } else {
        builder = new Builder(tempFile).withSchema(avroSchema);
      }
    } catch (SemanticVersion.SemanticVersionParseException e) {
      LOG.warn("Can't parse parquet version string: " + Version.VERSION_NUMBER, e);
      builder = new Builder(tempFile).withSchema(avroSchema);
    }

    // Generic arguments from the Job
    if(propertyDefined(conf, AvroParquetConstants.COMPRESSION_CODEC_NAME)) {
      String codec = conf.get(AvroParquetConstants.COMPRESSION_CODEC_NAME);
      LOG.info("Using compression codec: {}", codec);
      builder.withCompressionCodec(CompressionCodecName.fromConf(codec));
    }
    if(propertyDefined(conf, AvroParquetConstants.ROW_GROUP_SIZE)) {
      int size = conf.getInt(AvroParquetConstants.ROW_GROUP_SIZE, -1);
      LOG.info("Using row group size: {}", size);
      builder.withRowGroupSize(size);
    }
    if(propertyDefined(conf, AvroParquetConstants.PAGE_SIZE)) {
      int size = conf.getInt(AvroParquetConstants.PAGE_SIZE, -1);
      LOG.info("Using page size: {}", size);
      builder.withPageSize(size);
    }
    if(propertyDefined(conf, AvroParquetConstants.DICTIONARY_PAGE_SIZE)) {
      int size = conf.getInt(AvroParquetConstants.DICTIONARY_PAGE_SIZE, -1);
      LOG.info("Using dictionary page size: {}", size);
      builder.withDictionaryPageSize(size);
    }
    if(propertyDefined(conf, AvroParquetConstants.MAX_PADDING_SIZE)) {
      int size = conf.getInt(AvroParquetConstants.MAX_PADDING_SIZE, -1);
      LOG.info("Using max padding size: {}", size);
      builder.withMaxPaddingSize(size);
    }

    // Parquet writer
    ParquetWriter parquetWriter = builder
      .withConf(context.getConfiguration())
      .build();

    LOG.info("Started reading input file");
    long recordCount = 0;
    try {
      while (fileReader.hasNext()) {
        GenericRecord record = fileReader.next();
        parquetWriter.write(record);

        context.getCounter(Counters.PROCESSED_RECORDS).increment(1);
        recordCount++;
      }
    } catch(Exception e) {
      // Various random stuff can happen while converting, so we wrap the underlying exception with more details
      String message = "Exception at offset " + fileReader.tell() + " (record " + recordCount + "): " + e.toString();
      throw new IOException(message, e);
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
