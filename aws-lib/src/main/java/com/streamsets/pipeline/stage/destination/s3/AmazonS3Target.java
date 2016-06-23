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
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class AmazonS3Target extends BaseTarget {

  private final static Logger LOG = LoggerFactory.getLogger(AmazonS3Target.class);

  private static final String EL_PREFIX = "${";
  private static final String GZIP_EXTENSION = ".gz";
  private static final String PARTITION_TEMPLATE = "partitionTemplate";

  private final S3TargetConfigBean s3TargetConfigBean;
  private final String partitionTemplate;
  private ErrorRecordHandler errorRecordHandler;
  private ELEval partitionEval;
  private ELVars partitionVars;
  private int fileCount = 0;

  public AmazonS3Target(S3TargetConfigBean s3TargetConfigBean) {
    this.s3TargetConfigBean = s3TargetConfigBean;
    this.partitionTemplate = s3TargetConfigBean.partitionTemplate == null ? "" : s3TargetConfigBean.partitionTemplate;
  }

  @Override
  public List<ConfigIssue> init() {
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    partitionEval = getContext().createELEval(PARTITION_TEMPLATE);
    partitionVars = getContext().createELVars();

    List<ConfigIssue> issues = s3TargetConfigBean.init(getContext(), super.init());
    if (partitionTemplate.contains(EL_PREFIX)) {
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
    return issues;
  }

  @Override
  public void destroy() {
    s3TargetConfigBean.destroy();
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Multimap<String, Record> partitions = ELUtils.partitionBatchByExpression(
        partitionEval,
        partitionVars,
        partitionTemplate,
        batch
    );

    for (String partition : partitions.keySet()) {
      // commonPrefix always ends with a delimiter, so no need to append one to the end
      String keyPrefix = s3TargetConfigBean.s3Config.commonPrefix;
      // partition is optional
      if (!partition.isEmpty()) {
        keyPrefix += partition;
        if (!partition.endsWith(s3TargetConfigBean.s3Config.delimiter)) {
          keyPrefix += s3TargetConfigBean.s3Config.delimiter;
        }
      }
      keyPrefix += s3TargetConfigBean.fileNamePrefix + "-" + System.currentTimeMillis() + "-";

      Iterator<Record> records = partitions.get(partition).iterator();
      int writtenRecordCount = 0;
      DataGenerator generator;
      Record currentRecord;

      try {
        ByRefByteArrayOutputStream bOut = new ByRefByteArrayOutputStream();
        OutputStream out = bOut;

        // wrap with gzip compression output stream if required
        if(s3TargetConfigBean.compress) {
          out = new GZIPOutputStream(bOut);
        }

        generator = s3TargetConfigBean.getGeneratorFactory().getGenerator(out);
        while (records.hasNext()) {
          currentRecord = records.next();
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
          fileCount++;
          StringBuilder fileName = new StringBuilder();
          fileName = fileName.append(keyPrefix).append(fileCount);
          if(s3TargetConfigBean.compress) {
            fileName = fileName.append(GZIP_EXTENSION);
          }

          // Avoid making a copy of the internal buffer maintained by the ByteArrayOutputStream by using
          // ByRefByteArrayOutputStream
          ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bOut.getInternalBuffer(), 0, bOut.size());

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

          PutObjectRequest putObjectRequest = new PutObjectRequest(s3TargetConfigBean.s3Config.bucket,
              fileName.toString(), byteArrayInputStream, metadata);

          LOG.debug("Uploading object {} into Amazon S3", s3TargetConfigBean.s3Config.bucket +
              s3TargetConfigBean.s3Config.delimiter + fileName);
          s3TargetConfigBean.s3Config.getS3Client().putObject(putObjectRequest);
          LOG.debug("Successfully uploaded object {} into Amazon S3", s3TargetConfigBean.s3Config.bucket +
              s3TargetConfigBean.s3Config.delimiter + fileName);
        }
      } catch (AmazonClientException | IOException e) {
        LOG.error(Errors.S3_21.getMessage(), e.toString(), e);
        throw new StageException(Errors.S3_21, e.toString(), e);
      }
    }
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
