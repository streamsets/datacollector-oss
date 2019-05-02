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

import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

abstract class FileHelper {
  private static final Logger LOG = LoggerFactory.getLogger(FileHelper.class);

  protected static final String BUCKET = "bucket";
  protected static final String OBJECT_KEY = "objectKey";
  protected static final String RECORD_COUNT = "recordCount";

  private final TransferManager transferManager;

  protected final Target.Context context;
  protected final S3TargetConfigBean s3TargetConfigBean;
  protected final ErrorRecordHandler errorRecordHandler;

  FileHelper(Target.Context context, S3TargetConfigBean s3TargetConfigBean, TransferManager transferManager) {
    this.context = context;
    this.s3TargetConfigBean = s3TargetConfigBean;
    this.transferManager = transferManager;
    this.errorRecordHandler = new DefaultErrorRecordHandler(context);
  }

  abstract List<UploadMetadata> handle(Iterator<Record> recordIterator, String bucket, String keyPrefix) throws IOException, StageException;

  protected ObjectMetadata getObjectMetadata() throws StageException {
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
              s3TargetConfigBean.sseConfig.kmsKeyId.get()
          );
          String encryptionContext = s3TargetConfigBean.sseConfig.resolveAndEncodeEncryptionContext();
          if (encryptionContext != null) {
            metadata.setHeader("x-amz-server-side-encryption-context", encryptionContext);
          }
          break;
        case CUSTOMER:
          metadata.setSSECustomerAlgorithm(SSEAlgorithm.AES256.getAlgorithm());
          metadata.setHeader(
              Headers.SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
              s3TargetConfigBean.sseConfig.customerKey.get()
          );
          metadata.setHeader(
              Headers.COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
              s3TargetConfigBean.sseConfig.customerKeyMd5.get()
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

  Upload doUpload(String bucket, String fileName, InputStream is, ObjectMetadata metadata) {
    final PutObjectRequest putObjectRequest = new PutObjectRequest(
        bucket,
        fileName,
        is,
        metadata
    );
    final String object = bucket + s3TargetConfigBean.s3Config.delimiter + fileName;
    Upload upload = transferManager.upload(putObjectRequest);
    upload.addProgressListener((ProgressListener) progressEvent -> {
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
        default:
          break;
      }
    });
    return upload;
  }
}
