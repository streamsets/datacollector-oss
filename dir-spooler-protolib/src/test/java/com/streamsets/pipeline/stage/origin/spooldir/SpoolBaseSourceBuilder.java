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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class SpoolBaseSourceBuilder {
  SpoolDirConfigBean conf;

  public SpoolBaseSourceBuilder() {
    conf = new SpoolDirConfigBean();
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 10;
    conf.maxSpoolFiles = 10;
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.retentionTimeMins = 10;
  }

  public SpoolBaseSourceBuilder dataFormat(DataFormat dataFormat) {
    conf.dataFormat = dataFormat;
    return this;
  }

  public SpoolBaseSourceBuilder spoolDir(String spoolDir) {
    conf.spoolDir = spoolDir;
    return this;
  }

  public SpoolBaseSourceBuilder batchSize(int batchSize) {
    conf.batchSize = batchSize;
    return this;
  }

  public SpoolBaseSourceBuilder overrunLimit(int overrunLimit) {
    conf.overrunLimit = overrunLimit;
    return this;
  }

  public SpoolBaseSourceBuilder poolingTimeoutSecs(int poolingTimeoutSecs) {
    conf.poolingTimeoutSecs = poolingTimeoutSecs;
    return this;
  }

  public SpoolBaseSourceBuilder filePattern(String filePattern) {
    conf.filePattern = filePattern;
    return this;
  }

  public SpoolBaseSourceBuilder pathMatcherMode(PathMatcherMode pathMatcherMode) {
    conf.pathMatcherMode = pathMatcherMode;
    return this;
  }

  public SpoolBaseSourceBuilder maxSpoolFiles(int maxSpoolFiles) {
    conf.maxSpoolFiles = maxSpoolFiles;
    return this;
  }

  public SpoolBaseSourceBuilder initialFileToProcess(String initialFileToProcess) {
    conf.initialFileToProcess = initialFileToProcess;
    return this;
  }

  public SpoolBaseSourceBuilder errorArchiveDir(String errorArchiveDir) {
    conf.errorArchiveDir = errorArchiveDir;
    return this;
  }

  public SpoolBaseSourceBuilder postProcessing(PostProcessingOptions postProcessing) {
    conf.postProcessing = postProcessing;
    return this;
  }

  public SpoolBaseSourceBuilder archiveDir(String archiveDir) {
    conf.archiveDir = archiveDir;
    return this;
  }

  public SpoolBaseSourceBuilder retentionTimeMins(int retentionTimeMins) {
    conf.retentionTimeMins = retentionTimeMins;
    return this;
  }

  public SpoolBaseSourceBuilder dataFormatConfig(DataParserFormatConfig dataFormatConfig) {
    conf.dataFormatConfig = dataFormatConfig;
    return this;
  }

  public SpoolDirConfigBean getConf() {
    return conf;
  }
}
