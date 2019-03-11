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
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.lib.converter.AvroParquetConstants;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.Version;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AvroToParquetConverterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(AvroToParquetConverterUtil.class);
  private AvroToParquetConverterUtil() {}

  public static ParquetWriter.Builder initializeWriter(
      Path tempFile,
      Schema avroSchema,
      Configuration conf
  ) throws IOException {

    //private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Detect Parquet version to see if it supports logical types
    LOG.info("Detected Parquet version: " + Version.FULL_VERSION);
    ParquetWriter.Builder builder = getParquetWriterBuilder(tempFile, avroSchema, conf);

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

    return builder;
  }

  private static ParquetWriter.Builder getParquetWriterBuilder(Path tempFile, Schema avroSchema, Configuration conf) {
    // Parquet Avro pre-1.9 doesn't work with logical types, so in that case we use custom Builder that injects our own
    // avro schema -> parquet schema generator class (which is a copy of the one that was provided in PARQUET-358).
    // Additionally, Parquet Avro 1.9.x does not support converting from Avro timestamps (logical types TIMESTAMP_MILLIS
    // and TIMESTAMP_MICROS) and so we have to extend Parquet Avro classes to support timestamps conversion.
    ParquetWriter.Builder builder = null;
    try {
      SemanticVersion parquetVersion = SemanticVersion.parse(Version.VERSION_NUMBER);
      if(parquetVersion.major > 1 || (parquetVersion.major == 1 && parquetVersion.minor >= 9)) {
        if (parquetVersion.major == 1 && parquetVersion.minor >= 9) {
          LOG.debug("Creating AvroParquetWriterBuilder190Int96");
          if (propertyDefined(conf, AvroParquetConstants.TIMEZONE)) {
            String timeZoneId = conf.get(AvroParquetConstants.TIMEZONE);
            builder = new AvroParquetWriterBuilder190Int96(tempFile, timeZoneId).withSchema(avroSchema);
          } else {
            builder = new AvroParquetWriterBuilder190Int96(tempFile).withSchema(avroSchema);
          }
        } else {
          LOG.debug("Creating AvroParquetWriter.builder");
          builder = AvroParquetWriter.builder(tempFile).withSchema(avroSchema);
        }
      } else {
        LOG.debug("Creating AvroParquetWriterBuilder");
        builder = new AvroParquetWriterBuilder(tempFile).withSchema(avroSchema);
      }
    } catch (SemanticVersion.SemanticVersionParseException e) {
      LOG.warn("Can't parse parquet version string: " + Version.VERSION_NUMBER, e);
      builder = new AvroParquetWriterBuilder(tempFile).withSchema(avroSchema);
    }
    return builder;
  }

  // Return true if and only if given property is defined with non empty non default value
  private static boolean propertyDefined(Configuration conf, String propertyName) {
    String prop = conf.get(propertyName);
    // String property will have default empty, integer -1, we'll skip both of them
    return prop != null && !prop.isEmpty() && !prop.equals("-1");
  }
}
