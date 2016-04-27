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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
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

  private static final String GZIP_EXTENSION = ".gz";

  private final S3TargetConfigBean s3TargetConfigBean;
  private int fileCount = 0;

  public AmazonS3Target (S3TargetConfigBean s3TargetConfigBean) {
    this.s3TargetConfigBean = s3TargetConfigBean;
  }

  @Override
  public List<ConfigIssue> init() {
    return s3TargetConfigBean.init(getContext(), super.init());
  }

  @Override
  public void destroy() {
    s3TargetConfigBean.destroy();
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    String keyPrefix = s3TargetConfigBean.s3Config.commonPrefix + s3TargetConfigBean.fileNamePrefix + "-" +
      System.currentTimeMillis() + "-";
    Iterator<Record> records = batch.getRecords();
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
        } catch (IOException | StageException e) {
          handleException(e, currentRecord);
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

        PutObjectRequest putObjectRequest = new PutObjectRequest(s3TargetConfigBean.s3Config.bucket,
          fileName.toString(), byteArrayInputStream, null);

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

  private void handleException(Exception e, Record currentRecord) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().toError(currentRecord, e);
        break;
      case STOP_PIPELINE:
        if (e instanceof StageException) {
          throw (StageException) e;
        } else {
          throw new StageException(Errors.S3_32, currentRecord.getHeader().getSourceId(), e.toString(), e);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown OnErrorRecord option '{}'",
          getContext().getOnErrorRecord()));
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
