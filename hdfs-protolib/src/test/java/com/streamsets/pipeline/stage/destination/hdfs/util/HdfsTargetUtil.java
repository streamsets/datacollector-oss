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
package com.streamsets.pipeline.stage.destination.hdfs.util;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.destination.hdfs.CompressionMode;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsFileType;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsSequenceFileCompressionType;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsTarget;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsTargetConfigBean;
import com.streamsets.pipeline.stage.destination.hdfs.LateRecordsAction;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.Map;

public class HdfsTargetUtil {

  public static HdfsTarget createHdfsTarget(
      String hdfsUri,
      String hdfsUser,
      boolean hdfsKerberos,
      String hdfsConfDir,
      Map<String, String> hdfsConfigs,
      String uniquePrefix,
      String timeZoneID,
      String dirPathTemplate,
      HdfsFileType fileType,
      String keyEl,
      CompressionMode compression,
      HdfsSequenceFileCompressionType seqFileCompressionType,
      int maxRecordsPerFile,
      int maxFileSize,
      String timeDriver,
      String lateRecordsLimit,
      LateRecordsAction lateRecordsAction,
      String lateRecordsDirPathTemplate,
      DataFormat dataFormat,
      DataGeneratorFormatConfig dataGeneratorFormatConfig,
      String idleTimeout
  ) {
    HdfsTargetConfigBean hdfsTargetConfigBean = new HdfsTargetConfigBean();
    hdfsTargetConfigBean.hdfsUri = hdfsUri;
    hdfsTargetConfigBean.hdfsUser = hdfsUser;
    hdfsTargetConfigBean.hdfsKerberos = hdfsKerberos;
    hdfsTargetConfigBean.hdfsConfDir = hdfsConfDir;
    hdfsTargetConfigBean.hdfsConfigs = hdfsConfigs;
    hdfsTargetConfigBean.uniquePrefix = uniquePrefix;
    hdfsTargetConfigBean.timeZoneID = timeZoneID;
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
    return new HdfsTarget(hdfsTargetConfigBean);
  }
}
