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
package com.streamsets.pipeline.stage.destination.hdfs.util;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.stage.destination.hdfs.CompressionMode;
import com.streamsets.pipeline.stage.destination.hdfs.HadoopConfigBean;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsFileType;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsSequenceFileCompressionType;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsTarget;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsTargetConfigBean;
import com.streamsets.pipeline.stage.destination.hdfs.LateRecordsAction;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.LinkedList;
import java.util.List;

public class HdfsTargetUtil {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    String hdfsUri = "file:///";
    String hdfsUser = "foo";
    boolean hdfsKerberos = false;
    String hdfsConfDir = null;
    List<HadoopConfigBean> hdfsConfigs = new LinkedList<>();
    String uniquePrefix = "foo";
    String timeZoneID = "UTC";
    boolean dirPathTemplateInHeader = false;
    String dirPathTemplate = "/tmp/output/";
    HdfsFileType fileType = HdfsFileType.TEXT;
    String keyEl = "${uuid()}";
    CompressionMode compression = CompressionMode.NONE;
    HdfsSequenceFileCompressionType seqFileCompressionType = HdfsSequenceFileCompressionType.BLOCK;
    int maxRecordsPerFile = 5;
    int maxFileSize = 0;
    String timeDriver = "${time:now()}";
    String lateRecordsLimit = "${1 * SECONDS}";
    LateRecordsAction lateRecordsAction = LateRecordsAction.SEND_TO_ERROR;
    String lateRecordsDirPathTemplate = "";
    DataFormat dataFormat = DataFormat.SDC_JSON;
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    String idleTimeout = null;
    boolean rollIfHeader = false;
    String rollHeaderName = null;
    String fileNameEL = "";
    WholeFileExistsAction wholeFileExistsAction = WholeFileExistsAction.TO_ERROR;
    String permissionEL = "";
    boolean includeSchemaInEvents = false;
    ChecksumAlgorithm checksumAlgorithm = ChecksumAlgorithm.MD5;

    public HdfsTarget build() {
      HdfsTargetConfigBean hdfsTargetConfigBean = new HdfsTargetConfigBean();
      Utils.checkState(
          (dataFormat != DataFormat.WHOLE_FILE && fileNameEL.isEmpty()) || (dataFormat == DataFormat.WHOLE_FILE),
          "FileNameEL should be set only for Whole File Data format"
      );
      hdfsTargetConfigBean.hdfsUri = hdfsUri;
      hdfsTargetConfigBean.hdfsUser = hdfsUser;
      hdfsTargetConfigBean.hdfsKerberos = hdfsKerberos;
      hdfsTargetConfigBean.hdfsConfDir = hdfsConfDir;
      hdfsTargetConfigBean.hdfsConfigs = hdfsConfigs;
      hdfsTargetConfigBean.uniquePrefix = uniquePrefix;
      hdfsTargetConfigBean.timeZoneID = timeZoneID;
      hdfsTargetConfigBean.dirPathTemplateInHeader = dirPathTemplateInHeader;
      hdfsTargetConfigBean.dirPathTemplate = dirPathTemplate;
      hdfsTargetConfigBean.fileType = fileType;
      hdfsTargetConfigBean.keyEl = keyEl;
      hdfsTargetConfigBean.compression = compression;
      hdfsTargetConfigBean.seqFileCompressionType = seqFileCompressionType;
      hdfsTargetConfigBean.maxRecordsPerFile = maxRecordsPerFile;
      hdfsTargetConfigBean.maxFileSize = maxFileSize;
      hdfsTargetConfigBean.timeDriver = timeDriver;
      hdfsTargetConfigBean.lateRecordsLimit = lateRecordsLimit;
      hdfsTargetConfigBean.lateRecordsAction = lateRecordsAction;
      hdfsTargetConfigBean.lateRecordsDirPathTemplate = lateRecordsDirPathTemplate;
      hdfsTargetConfigBean.dataFormat = dataFormat;
      hdfsTargetConfigBean.dataGeneratorFormatConfig = dataGeneratorFormatConfig;
      hdfsTargetConfigBean.idleTimeout = idleTimeout;
      hdfsTargetConfigBean.rollIfHeader = rollIfHeader;
      hdfsTargetConfigBean.rollHeaderName = rollHeaderName;
      hdfsTargetConfigBean.dataGeneratorFormatConfig.fileNameEL = fileNameEL;
      hdfsTargetConfigBean.dataGeneratorFormatConfig.wholeFileExistsAction = wholeFileExistsAction;
      hdfsTargetConfigBean.permissionEL = permissionEL;
      hdfsTargetConfigBean.dataGeneratorFormatConfig.includeChecksumInTheEvents = includeSchemaInEvents;
      hdfsTargetConfigBean.dataGeneratorFormatConfig.checksumAlgorithm = checksumAlgorithm;
      return new HdfsTarget(hdfsTargetConfigBean);
    }

    public Builder hdfsUri(String hdfsUri) {
      this.hdfsUri = hdfsUri;
      return this;
    }

    public Builder hdfsUser(String hdfsUser) {
      this.hdfsUser = hdfsUser;
      return this;
    }

    public Builder hdfsKerberos(boolean hdfsKerberos) {
      this.hdfsKerberos = hdfsKerberos;
      return this;
    }

    public Builder hdfsConfDir(String dir) {
      this.hdfsConfDir = dir;
      return this;
    }

    public Builder addHdfsConfig(String key, String value) {
      this.hdfsConfigs.add(new HadoopConfigBean(key, value));
      return this;
    }

    public Builder uniquePrefix(String prefix) {
      this.uniquePrefix = prefix;
      return this;
    }

    public Builder timeZoneId(String timeZoneID) {
      this.timeZoneID = timeZoneID;
      return this;
    }

    public Builder dirPathTemplateInHeader(boolean inHeader) {
      this.dirPathTemplateInHeader = inHeader;
      return this;
    }

    public Builder dirPathTemplate(String template) {
      this.dirPathTemplate = template;
      return this;
    }

    public Builder fileType(HdfsFileType type) {
      this.fileType = type;
      return this;
    }

    public Builder keyEl(String el) {
      this.keyEl = el;
      return this;
    }

    public Builder compression(CompressionMode mode) {
      this.compression = mode;
      return this;
    }

    public Builder seqFileCompressionType(HdfsSequenceFileCompressionType type) {
      this.seqFileCompressionType = type;
      return this;
    }

    public Builder maxRecordsPerFile(int size) {
      this.maxRecordsPerFile = size;
      return this;
    }

    public Builder maxFileSize(int size) {
      this.maxFileSize = size;
      return this;
    }

    public Builder timeDriver(String timeDriver) {
      this.timeDriver = timeDriver;
      return this;
    }

    public Builder lateRecordsLimit(String limit) {
      this.lateRecordsLimit = limit;
      return this;
    }

    public Builder lateRecordsAction(LateRecordsAction action) {
      this.lateRecordsAction = action;
      return this;
    }

    public Builder lateRecordsDirPathTemplate(String template) {
      this.lateRecordsDirPathTemplate = template;
      return this;
    }

    public Builder dataForamt(DataFormat format) {
      this.dataFormat = format;
      return this;
    }

    public Builder dataGeneratorFormatConfig(DataGeneratorFormatConfig config) {
      this.dataGeneratorFormatConfig = config;
      return this;
    }

    public Builder idleTimeout(String idleTimeout) {
      this.idleTimeout = idleTimeout;
      return this;
    }

    public Builder rollIfHeader(boolean rollIfHeader) {
      this.rollIfHeader = rollIfHeader;
      return this;
    }

   public Builder rollHeaderName(String rollHeaderName) {
     this.rollHeaderName = rollHeaderName;
      return this;
    }

    public Builder fileNameEL(String fileNameEL) {
      this.fileNameEL = fileNameEL;
      return this;
    }

    public Builder wholeFileExistsAction(WholeFileExistsAction wholeFileExistsAction) {
      this.wholeFileExistsAction = wholeFileExistsAction;
      return this;
    }

    public Builder permissionEL(String permissionEL) {
      this.permissionEL = permissionEL;
      return this;
    }

    public Builder includeChecksumInTheEvents(boolean includeSchemaInEvents) {
      this.includeSchemaInEvents = includeSchemaInEvents;
      return this;
    }

    public Builder checksumAlgorithm(ChecksumAlgorithm checksumAlgorithm) {
      this.checksumAlgorithm = checksumAlgorithm;
      return this;
    }
  }
}
