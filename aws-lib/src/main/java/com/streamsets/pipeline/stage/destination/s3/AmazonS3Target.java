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
package com.streamsets.pipeline.stage.destination.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Executors;

public class AmazonS3Target extends BaseTarget {

  private final static Logger LOG = LoggerFactory.getLogger(AmazonS3Target.class);

  private static final String EL_PREFIX = "${";
  private static final String BUCKET_TEMPLATE = "bucketTemplate";
  private static final String PARTITION_TEMPLATE = "partitionTemplate";
  private static final String TIME_DRIVER_TEMPLATE = "timeDriverTemplate";
  private static final String BUCKET_DOES_NOT_EXIST = "The specified bucket does not exist";

  private final S3TargetConfigBean s3TargetConfigBean;
  private final String bucketTemplate;
  private final String partitionTemplate;
  private final String timeDriverTemplate;

  private FileHelper fileHelper;
  private TransferManager transferManager;
  private ELEval bucketEval;
  private ELEval partitionEval;
  private ELEval timeDriverEval;
  private ELVars elVars;
  private Calendar calendar;

  private ErrorRecordHandler errorRecordHandler;

  public AmazonS3Target(S3TargetConfigBean s3TargetConfigBean) {
    this.s3TargetConfigBean = s3TargetConfigBean;
    this.partitionTemplate = s3TargetConfigBean.partitionTemplate == null ? "" : s3TargetConfigBean.partitionTemplate;
    this.timeDriverTemplate = s3TargetConfigBean.timeDriverTemplate == null ? "" : s3TargetConfigBean.timeDriverTemplate;
    this.bucketTemplate = s3TargetConfigBean.s3Config.bucketTemplate;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = s3TargetConfigBean.init(getContext(), super.init());

    elVars = getContext().createELVars();
    bucketEval = getContext().createELEval(BUCKET_TEMPLATE);
    partitionEval = getContext().createELEval(PARTITION_TEMPLATE);
    timeDriverEval = getContext().createELEval(TIME_DRIVER_TEMPLATE);
    calendar = Calendar.getInstance(TimeZone.getTimeZone(s3TargetConfigBean.timeZoneID));

    transferManager = TransferManagerBuilder
        .standard()
        .withS3Client(s3TargetConfigBean.s3Config.getS3Client())
        .withExecutorFactory(() -> Executors.newFixedThreadPool(s3TargetConfigBean.tmConfig.threadPoolSize))
        .withMinimumUploadPartSize(s3TargetConfigBean.tmConfig.minimumUploadPartSize)
        .withMultipartUploadThreshold(s3TargetConfigBean.tmConfig.multipartUploadThreshold)
        .build();

    TimeEL.setCalendarInContext(elVars, calendar);
    TimeNowEL.setTimeNowInContext(elVars, new Date());

    if (partitionTemplate.contains(EL_PREFIX)) {
      ELUtils.validateExpression(
          partitionEval,
          elVars,
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
      ELUtils.validateExpression(
          timeDriverEval,
          elVars,
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

    this.errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    return issues;
  }

  @Override
  public void destroy() {
    s3TargetConfigBean.s3Config.destroy();
    if (transferManager != null) {
      // don't shut down s3 client again since it's already closed by s3Config.destroy().
      transferManager.shutdownNow(false);
    }
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Multimap<Partition, Record> partitions = partitionBatch(batch);

    try {
      List<UploadMetadata> uploads = new ArrayList<>();
      for (Partition partition : partitions.keySet()) {
        List<UploadMetadata> partitionUploads = fileHelper.handle(
          partitions.get(partition).iterator(),
          partition.bucket,
          getKeyPrefix(partition.path)
        );
        uploads.addAll(partitionUploads);
      }

      for (UploadMetadata upload : uploads) {
        try {
          // Wait for given object to fully upload
          upload.getUpload().waitForCompletion();

          // Propagate events associated with this upload
          for(EventRecord event : upload.getEvents()) {
            getContext().toEvent(event);
          }
        } catch (AmazonClientException | InterruptedException e) {
          LOG.error(Errors.S3_21.getMessage(), e.toString(), e);

          // Sadly Amazon does not provide a better way how to determine what has happened
          if(e instanceof AmazonClientException && e.toString().contains(BUCKET_DOES_NOT_EXIST)) {
            // In case of incorrect bucket, we simply move all records to error stream
            errorRecordHandler.onError(upload.getRecords(), new StageException(Errors.S3_21, e.toString()));
          } else {
            // In default case we stop pipeline execution (incorrect credentials, network split, ...)
            throw new StageException(Errors.S3_21, e.toString(), e);
          }
        }
      }

    } catch (IOException e) {
      // IOException is hard exception on which we will stop pipeline
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

  private static class Partition {
    final String bucket;
    final String path;

    public Partition(String bucket, String path) {
      this.bucket = Preconditions.checkNotNull(bucket);
      this.path = Preconditions.checkNotNull(path);
    }

    @Override
    public boolean equals(Object o) {
      Partition other = (Partition) o;
      return bucket.equals(other.bucket) && path.equals(other.path);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(bucket, path);
    }
  }

  public Multimap<Partition, Record> partitionBatch(
    Batch batch
  ) throws StageException {
    Multimap<Partition, Record> partitions = ArrayListMultimap.create();

    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      RecordEL.setRecordInContext(elVars, record);

      Date recordTime = ELUtils.getRecordTime(
        timeDriverEval,
        elVars,
        timeDriverTemplate,
        record
      );

      calendar.setTime(recordTime);
      TimeEL.setCalendarInContext(elVars, calendar);
      TimeNowEL.setTimeNowInContext(elVars, recordTime);

      try {
        String bucketName = bucketEval.eval(elVars, bucketTemplate, String.class);
        String pathName = partitionEval.eval(elVars, partitionTemplate, String.class);

        if(Strings.isNullOrEmpty(bucketName)) {
          throw new OnRecordErrorException(record, Errors.S3_01, record.getHeader().getSourceId());
        }

        LOG.debug("Evaluated record '{}' to bucket '{}' and partition '{}'", record.getHeader().getSourceId(), bucketName, pathName);
        partitions.put(new Partition(bucketName, pathName), record);
      } catch (OnRecordErrorException e) {
        errorRecordHandler.onError(e);
      } catch (ELEvalException e) {
        LOG.error("Failed to evaluate expression: ", e.toString(), e);
        throw new StageException(e.getErrorCode(), e.getParams());
      }
    }

    return partitions;
  }

}
