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
package com.streamsets.pipeline.stage.processor.transformer;

import com.streamsets.pipeline.lib.converter.AvroParquetConfig;

public class TestWholeFileTransformerProcessorBuilder {
  JobConfig jobConfig = new JobConfig();
  AvroParquetConfig avroParquetConfig = new AvroParquetConfig();

  public TestWholeFileTransformerProcessorBuilder() {
    jobConfig.jobType = JobType.AVRO_PARQUET;
    jobConfig.tempDir = "${file:parentPath(record:attribute('file'))}/.parquet";
    jobConfig.uniquePrefix = ".avro_to_parquet_tmp_conversion_";
    jobConfig.fileNameSuffix = ".parquet";

    jobConfig.wholeFileMaxObjectLen = 8 * 1024;
    jobConfig.rateLimit = "-1";
  }

  public TestWholeFileTransformerProcessorBuilder jobType(JobType jobType){
    jobConfig.jobType = jobType;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder tempDir(String tempDir) {
    jobConfig.tempDir = tempDir;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder uniquePrefix(String uniquePrefix) {
    jobConfig.uniquePrefix = uniquePrefix;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder fileNameSuffix(String fileNameSuffix) {
    jobConfig.fileNameSuffix = fileNameSuffix;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder wholeFileMaxObjectLen(int wholeFileMaxObjectLen) {
    jobConfig.wholeFileMaxObjectLen = wholeFileMaxObjectLen;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder rateLimit(String rateLimit) {
    jobConfig.rateLimit = rateLimit;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder compressionCodec (String compressionCodec) {
    avroParquetConfig.compressionCodec = compressionCodec;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder rowGroupSize (int rowGroupSize) {
    avroParquetConfig.rowGroupSize = rowGroupSize;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder pageSize (int pageSize) {
    avroParquetConfig.pageSize = pageSize;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder dictionaryPageSize (int dictionaryPageSize) {
    avroParquetConfig.dictionaryPageSize = dictionaryPageSize;
    return this;
  }

  public TestWholeFileTransformerProcessorBuilder maxPaddingSize (int maxPaddingSize) {
    avroParquetConfig.maxPaddingSize = maxPaddingSize;
    return this;
  }

  public WholeFileTransformerProcessor build() {
    jobConfig.avroParquetConfig = avroParquetConfig;
    return  new WholeFileTransformerProcessor(jobConfig);
  }
}
