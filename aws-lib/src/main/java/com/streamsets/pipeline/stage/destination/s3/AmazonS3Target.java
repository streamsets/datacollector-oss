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
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

public class AmazonS3Target extends BaseTarget {

  private final static Logger LOG = LoggerFactory.getLogger(AmazonS3Target.class);

  private static final String EL_PREFIX = "${";
  private static final String GZIP_EXTENSION = ".gz";
  private static final String PARTITION_TEMPLATE = "partitionTemplate";
  private static final String TIME_DRIVER_TEMPLATE = "timeDriverTemplate";

  private final S3TargetConfigBean s3TargetConfigBean;
  private final String partitionTemplate;
  private final String timeDriverTemplate;
  private ErrorRecordHandler errorRecordHandler;
  private TransferManager transferManager;
  private ELEval partitionEval;
  private ELVars partitionVars;
  private ELEval timeDriverEval;
  private ELVars timeDriverVars;
  private Calendar calendar;
  private int fileCount = 0;
  private ELEval fileNameELEval;

  public AmazonS3Target(S3TargetConfigBean s3TargetConfigBean) {
    this.s3TargetConfigBean = s3TargetConfigBean;
    this.partitionTemplate = s3TargetConfigBean.partitionTemplate == null ? "" : s3TargetConfigBean.partitionTemplate;
    this.timeDriverTemplate = s3TargetConfigBean.timeDriverTemplate == null ? "" : s3TargetConfigBean.timeDriverTemplate;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = s3TargetConfigBean.init(getContext(), super.init());

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
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
      initForWholeFile(issues);
    }
    return issues;
  }

  private void initForWholeFile(List<ConfigIssue> issues) {
    fileNameELEval = getContext().createELEval("fileNameEL");
    if (s3TargetConfigBean.compress) {
      issues.add(
          getContext().createConfigIssue(
              Groups.S3.getLabel(),
              S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "compress",
              Errors.S3_50
          )
      );
    }
    //This initializes the metrics for the stage when the data format is whole file.
    DataGeneratorFactory dgFactory = s3TargetConfigBean.dataGeneratorFormatConfig.getDataGeneratorFactory();
    try (DataGenerator dg = dgFactory.getGenerator(new ByteArrayOutputStream())) {
      //NOOP
    } catch (IOException e) {
      issues.add(
          getContext().createConfigIssue(
              Groups.S3.getLabel(),
              S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataFormat",
              Errors.S3_40,
              e.getMessage()
          )
      );
    }
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
        List<Upload> partitionUploads;
        if (s3TargetConfigBean.dataFormat == DataFormat.WHOLE_FILE) {
          partitionUploads = handleRecordsForWholeFileFormat(partitions.get(partition).iterator(), keyPrefix);
        } else {
          partitionUploads = handleRecordsForNonWholeFileFormat(partitions.get(partition).iterator(), keyPrefix);
        }
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

  private void checkForWholeFileExistence(String objectKey) throws OnRecordErrorException {
    boolean fileExists = s3TargetConfigBean.s3Config.getS3Client().doesObjectExist(s3TargetConfigBean.s3Config.bucket, objectKey);
    WholeFileExistsAction wholeFileExistsAction = s3TargetConfigBean.dataGeneratorFormatConfig.wholeFileExistsAction;
    if (fileExists && wholeFileExistsAction == WholeFileExistsAction.TO_ERROR) {
      throw new OnRecordErrorException(Errors.S3_51, objectKey);
    }
    //else we will do the upload which will overwrite/ version based on the bucket configuration.
  }

  private List<Upload> handleRecordsForWholeFileFormat(
      Iterator<Record> recordIterator,
      String keyPrefix
  ) throws StageException, IOException {
    List<Upload> uploads = new ArrayList<>();
    if (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      String fileName = getFileNameFromFileNameEL(keyPrefix, record);
      try {
        checkForWholeFileExistence(fileName);
        FileRef fileRef = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef();
        ObjectMetadata metadata = getObjectMetadata();
        metadata = (metadata == null)? new ObjectMetadata() : metadata;
        //Mandatory field path specifying size.
        metadata.setContentLength(record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/size").getValueAsLong());

        //We are bypassing the generator because S3 has a convenient notion of taking input stream as a parameter.
        Upload upload = doUpload(fileName, fileRef.createInputStream(getContext(), InputStream.class), metadata);
        uploads.add(upload);
      } catch (OnRecordErrorException e) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                e.getErrorCode(),
                e.getParams()
            )
        );
      }
    }
    return uploads;
  }

  private List<Upload> handleRecordsForNonWholeFileFormat(
      Iterator<Record> recordIterator,
      String keyPrefix
  ) throws IOException, StageException {
    //For uniqueness
    keyPrefix += System.currentTimeMillis() + "-";

    int writtenRecordCount = 0;
    List<Upload> uploads = new ArrayList<>();

    ByRefByteArrayOutputStream bOut = new ByRefByteArrayOutputStream();
    // wrap with gzip compression output stream if required
    OutputStream out = (s3TargetConfigBean.compress)? new GZIPOutputStream(bOut) : bOut;

    DataGenerator generator = s3TargetConfigBean.getGeneratorFactory().getGenerator(out);
    Record currentRecord;
    while (recordIterator.hasNext()) {
      currentRecord = recordIterator.next();
      try {
        generator.write(currentRecord);
        writtenRecordCount++;
      } catch (StageException e) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                currentRecord,
                e.getErrorCode(),
                e.getParams()
            )
        );
      } catch (IOException e) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                currentRecord,
                Errors.S3_32,
                currentRecord.getHeader().getSourceId(),
                e.toString(),
                e
            )
        );
      }
    }
    generator.close();

    // upload file on Amazon S3 only if at least one record was successfully written to the stream
    if (writtenRecordCount > 0) {
      String fileName = getUniqueDateWithIncrementalFileName(keyPrefix);
      // Avoid making a copy of the internal buffer maintained by the ByteArrayOutputStream by using
      // ByRefByteArrayOutputStream
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bOut.getInternalBuffer(), 0, bOut.size());
      ObjectMetadata metadata = getObjectMetadata();
      Upload upload = doUpload(fileName, byteArrayInputStream, metadata);
      uploads.add(upload);
    }
    return uploads;
  }

  private String getUniqueDateWithIncrementalFileName(String keyPrefix) {
    fileCount++;
    StringBuilder fileName = new StringBuilder();
    fileName = fileName.append(keyPrefix).append(fileCount);
    if (s3TargetConfigBean.compress) {
      fileName = fileName.append(GZIP_EXTENSION);
    }
    return fileName.toString();
  }

  private String getFileNameFromFileNameEL(String keyPrefix, Record record) throws StageException {
    Utils.checkState(fileNameELEval != null, "File Name EL Evaluator is null");
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, record);
    StringBuilder fileName = new StringBuilder();
    fileName = fileName.append(keyPrefix);
    fileName.append(fileNameELEval.eval(vars, s3TargetConfigBean.dataGeneratorFormatConfig.fileNameEL, String.class));
    return fileName.toString();
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
    return keyPrefix + s3TargetConfigBean.fileNamePrefix + "-";
  }

  private ObjectMetadata getObjectMetadata() {
    ObjectMetadata metadata = null;
    if (s3TargetConfigBean.sseConfig.useSSE) {
      metadata = new ObjectMetadata();
      switch (s3TargetConfigBean.sseConfig.encryption) {
        case S3:
          metadata.setSSEAlgorithm(SSEAlgorithm.AES256.getAlgorithm());
          break;
        case KMS:
          metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
          metadata.setHeader(
              Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
              s3TargetConfigBean.sseConfig.kmsKeyId
          );
          if (!s3TargetConfigBean.sseConfig.encryptionContext.isEmpty()) {
            metadata.setHeader(
                "x-amz-server-side-encryption-context",
                s3TargetConfigBean.sseConfig.encryptionContext
            );
          }
          break;
        case CUSTOMER:
          metadata.setSSECustomerAlgorithm(SSEAlgorithm.AES256.getAlgorithm());
          metadata.setHeader(
              Headers.SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
              s3TargetConfigBean.sseConfig.customerKey
          );
          metadata.setHeader(
              Headers.COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
              s3TargetConfigBean.sseConfig.customerKeyMd5
          );
          break;
        default:
          throw new IllegalStateException(
              Utils.format(
                  "Unknown encryption option: ",
                  s3TargetConfigBean.sseConfig.encryption
              )
          );
      }
    }
    return metadata;
  }

  private Upload doUpload(String fileName, InputStream is, ObjectMetadata metadata) {
    final PutObjectRequest putObjectRequest = new PutObjectRequest(
        s3TargetConfigBean.s3Config.bucket,
        fileName,
        is,
        metadata
    );
    final String object = s3TargetConfigBean.s3Config.bucket + s3TargetConfigBean.s3Config.delimiter + fileName;
    Upload upload = transferManager.upload(putObjectRequest);
    upload.addProgressListener(
        new ProgressListener() {
          public void progressChanged(ProgressEvent progressEvent) {
            switch (progressEvent.getEventType()) {
              case TRANSFER_STARTED_EVENT:
                LOG.debug("Started uploading object {} into Amazon S3", object);
                break;
              case TRANSFER_COMPLETED_EVENT:
                LOG.debug("Completed uploading object {} into Amazon S3", object);
                break;
              case TRANSFER_FAILED_EVENT:
                LOG.debug("Failed uploading object {} into Amazon S3", object);
                break;
            }
          }
        }
    );
    return upload;
  }

  /**
   * Subclass of ByteArrayOutputStream which exposed the internal buffer to help avoid making a copy of the buffer.
   *
   * Note that the buffer size may be greater than the actual data. Therefore use {@link #size()} method to determine
   * the actual size of data.
   */
  private static class ByRefByteArrayOutputStream extends ByteArrayOutputStream {
    public byte[] getInternalBuffer() {
      return buf;
    }
  }

}
