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
package com.streamsets.pipeline.stage.origin.hdfs;

import com.streamsets.pipeline.config.DataFormat;

import java.util.ArrayList;

public class TestHdfsSourceBuilder {
  String hdfsUri = "file:///";
  int numberOfThreads = 1;
  int maxBatchSize = 1000;
  long poolingTimeoutSecs = 1;
  String dirPathTemplate = "/tmp/output/";
  String pattern = "*";
  DataFormat dataFormat = DataFormat.SDC_JSON;
  String firstFile = "";

  public TestHdfsSourceBuilder dirPathTemplate(String dirPathTemplate) {
    this.dirPathTemplate = dirPathTemplate;
    return this;
  }

  public TestHdfsSourceBuilder pattern(String pattern) {
    this.pattern = pattern;
    return this;
  }

  public TestHdfsSourceBuilder numberOfThreads(int numberOfThreads) {
    this.numberOfThreads = numberOfThreads;
    return this;
  }

  public TestHdfsSourceBuilder maxBatchSize(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
    return this;
  }

  public TestHdfsSourceBuilder poolingTimeoutSecs(long poolingTimeoutSecs) {
    this.poolingTimeoutSecs = poolingTimeoutSecs;
    return this;
  }

  public TestHdfsSourceBuilder dataFormat(DataFormat dataFormat) {
    this.dataFormat = dataFormat;
    return this;
  }

  public TestHdfsSourceBuilder hdfsUri(String hdfsUri) {
    this.hdfsUri = hdfsUri;
    return this;
  }

  public TestHdfsSourceBuilder firstFile(String firstFile) {
    this.firstFile = firstFile;
    return this;
  }

  public HdfsSource build() {
    HdfsSourceConfigBean hdfsSourceConfigBean = new HdfsSourceConfigBean();
    hdfsSourceConfigBean.dirPath = dirPathTemplate;
    hdfsSourceConfigBean.pattern = pattern;
    hdfsSourceConfigBean.numberOfThreads = numberOfThreads;
    hdfsSourceConfigBean.maxBatchSize = maxBatchSize;
    hdfsSourceConfigBean.poolingTimeoutSecs = poolingTimeoutSecs;
    hdfsSourceConfigBean.dataFormat = dataFormat;
    hdfsSourceConfigBean.hdfsUri = hdfsUri;
    hdfsSourceConfigBean.hdfsConfigs = new ArrayList<>();
    hdfsSourceConfigBean.firstFile = firstFile;

    return new HdfsSource(hdfsSourceConfigBean);
  }
}
