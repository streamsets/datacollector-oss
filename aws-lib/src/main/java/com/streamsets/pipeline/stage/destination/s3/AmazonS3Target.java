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
package com.streamsets.pipeline.stage.destination.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Executors;

public class AmazonS3Target extends BaseTarget {

  private final static Logger LOG = LoggerFactory.getLogger(AmazonS3Target.class);

  private static final String EL_PREFIX = "${";
  private static final String PARTITION_TEMPLATE = "partitionTemplate";
  private static final String TIME_DRIVER_TEMPLATE = "timeDriverTemplate";

  private final S3TargetConfigBean s3TargetConfigBean;
  private final String partitionTemplate;
  private final String timeDriverTemplate;

  private FileHelper fileHelper;
  private TransferManager transferManager;
  private ELEval partitionEval;
  private ELVars partitionVars;
  private ELEval timeDriverEval;
  private ELVars timeDriverVars;
  private Calendar calendar;

  public AmazonS3Target(S3TargetConfigBean s3TargetConfigBean) {
    this.s3TargetConfigBean = s3TargetConfigBean;
    this.partitionTemplate = s3TargetConfigBean.partitionTemplate == null ? "" : s3TargetConfigBean.partitionTemplate;
    this.timeDriverTemplate = s3TargetConfigBean.timeDriverTemplate == null ? "" : s3TargetConfigBean.timeDriverTemplate;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = s3TargetConfigBean.init(getContext(), super.init());

    partitionEval = getContext().createELEval(PARTITION_TEMPLATE);
    partitionVars = getContext().createELVars();
    timeDriverEval = getContext().createELEval(TIME_DRIVER_TEMPLATE);
    timeDriverVars = getContext().createELVars();
    calendar = Calendar.getInstance(TimeZone.getTimeZone(s3TargetConfigBean.timeZoneID));

    transferManager = new TransferManager(
        s3TargetConfigBean.s3Config.getS3Client(),
        Executors.newFixedThreadPool(s3TargetConfigBean.tmConfig.threadPoolSize)
    );
    TransferManagerConfiguration transferManagerConfiguration = new TransferManagerConfiguration();
    transferManagerConfiguration.setMinimumUploadPartSize(s3TargetConfigBean.tmConfig.minimumUploadPartSize);
    transferManagerConfiguration.setMultipartUploadThreshold(s3TargetConfigBean.tmConfig.multipartUploadThreshold);
    transferManager.setConfiguration(transferManagerConfiguration);

    if (partitionTemplate.contains(EL_PREFIX)) {
      TimeEL.setCalendarInContext(partitionVars, calendar);
      TimeNowEL.setTimeNowInContext(partitionVars, new Date());

      ELUtils.validateExpression(
          partitionEval,
          partitionVars,
          partitionTemplate,
          getContext(),
          Groups.S3.getLabel(),
          S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + PARTITION_TEMPLATE,
          Errors.S3_03,
          String.class,
          issues
      );
    }

    if (timeDriverTemplate.contains(EL_PREFIX)) {
      TimeEL.setCalendarInContext(timeDriverVars, calendar);
      TimeNowEL.setTimeNowInContext(timeDriverVars, new Date());

      ELUtils.validateExpression(
          timeDriverEval,
          timeDriverVars,
          timeDriverTemplate,
          getContext(),
          Groups.S3.getLabel(),
          S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + TIME_DRIVER_TEMPLATE,
          Errors.S3_04,
          Date.class,
          issues
      );
    }
    if (s3TargetConfigBean.dataFormat == DataFormat.WHOLE_FILE) {
      fileHelper = new WholeFileHelper(getContext(), s3TargetConfigBean, transferManager, issues);
    } else {
      fileHelper = new DefaultFileHelper(getContext(), s3TargetConfigBean, transferManager);
    }
    return issues;
  }

  @Override
  public void destroy() {
    s3TargetConfigBean.s3Config.destroy();
    // don't shut down s3 client again since it's already closed by s3Config.destroy().
    transferManager.shutdownNow(false);
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Multimap<String, Record> partitions = ELUtils.partitionBatchByExpression(
        partitionEval,
        partitionVars,
        partitionTemplate,
        timeDriverEval,
        timeDriverVars,
        timeDriverTemplate,
        calendar,
        batch
    );

    try {
      List<Upload> uploads = new ArrayList<>();
      for (String partition : partitions.keySet()) {
        String keyPrefix = getKeyPrefix(partition);
        List<Upload> partitionUploads = fileHelper.handle(partitions.get(partition).iterator(), keyPrefix);
        uploads.addAll(partitionUploads);
      }
      // Wait for all the uploads to complete before moving on to the next batch
      for (Upload upload : uploads) {
        upload.waitForCompletion();
      }
    } catch (AmazonClientException | IOException | InterruptedException e) {
      LOG.error(Errors.S3_21.getMessage(), e.toString(), e);
      throw new StageException(Errors.S3_21, e.toString(), e);
    }
  }

  private String getKeyPrefix(String partition) {
    // commonPrefix always ends with a delimiter, so no need to append one to the end
    String keyPrefix = s3TargetConfigBean.s3Config.commonPrefix;
    // partition is optional
    if (!partition.isEmpty()) {
      keyPrefix += partition;
      if (!partition.endsWith(s3TargetConfigBean.s3Config.delimiter)) {
        keyPrefix += s3TargetConfigBean.s3Config.delimiter;
      }
    }
    if (s3TargetConfigBean.fileNamePrefix != null && !s3TargetConfigBean.fileNamePrefix.isEmpty()) {
      //Append "-" to the key prefix only if fileNamePrefix is not empty.
      keyPrefix = keyPrefix + s3TargetConfigBean.fileNamePrefix + "-";
    }
    return keyPrefix;
  }

}
