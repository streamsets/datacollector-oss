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
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.ObjectLengthException;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class AmazonS3Source extends AbstractAmazonS3Source {

  private final static Logger LOG = LoggerFactory.getLogger(AmazonS3Source.class);
  private static final long DEFAULT_FETCH_SIZE = 1 * 1024 * 1024;

  private DataParser parser;
  private S3Object object;

  public AmazonS3Source(S3ConfigBean s3ConfigBean) {
    super(s3ConfigBean);
  }

  @Override
  protected void initChild(List<ConfigIssue> issues) {
    // nothing required here today
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(parser);
    super.destroy();
  }

  @Override
  protected String produce(S3ObjectSummary s3Object, String offset, int maxBatchSize, BatchMaker batchMaker)
    throws StageException, BadSpoolObjectException {
    try {
      if (parser == null) {
        //Get S3 object instead of stream because we want to call close on the object when we close the
        // parser (and stream)
        if(getContext().isPreview()) {
          long fetchSize = s3Object.getSize() > DEFAULT_FETCH_SIZE ? DEFAULT_FETCH_SIZE : s3Object.getSize();
          if(fetchSize > 0) {
            object = AmazonS3Util.getObjectRange(s3ConfigBean.s3Config.getS3Client(), s3ConfigBean.s3Config.bucket,
              s3Object.getKey(), fetchSize);
          }  else {
            LOG.warn("Size of object with key '{}' is 0", s3Object.getKey());
            object = AmazonS3Util.getObject(s3ConfigBean.s3Config.getS3Client(), s3ConfigBean.s3Config.bucket,
              s3Object.getKey());
          }
        } else {
          object = AmazonS3Util.getObject(s3ConfigBean.s3Config.getS3Client(), s3ConfigBean.s3Config.bucket,
            s3Object.getKey());
        }
        String recordId = s3ConfigBean.s3Config.bucket + s3ConfigBean.s3Config.delimiter + s3Object.getKey();
        parser = s3ConfigBean.dataFormatConfig.getParserFactory().getParser(recordId, object.getObjectContent(),
          offset);
        //we don't use S3 GetObject range capabilities to skip the already process offset because the parsers cannot
        // pick up from a non root doc depth in the case of a single object with records.
      }
      for (int i = 0; i < maxBatchSize; i++) {
        try {
          Record record = parser.parse();
          if (record != null) {
            batchMaker.addRecord(record);
            offset = parser.getOffset();
          } else {
            parser.close();
            parser = null;
            object.close();
            object = null;
            offset = S3Constants.MINUS_ONE;
            break;
          }
        } catch (ObjectLengthException ex) {
          String exOffset = offset;
          offset = S3Constants.MINUS_ONE;
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().reportError(Errors.S3_SPOOLDIR_02, s3Object.getKey(), exOffset, ex);
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.S3_SPOOLDIR_02, s3Object.getKey(), exOffset);
            default:
              throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                getContext().getOnErrorRecord(), ex));
          }
        }
      }
    } catch (AmazonClientException e) {
      LOG.error("Error processing object with key '{}' offset '{}'", s3Object.getKey(), offset, e);
      throw new StageException(Errors.S3_SPOOLDIR_25, e.toString(), e);
    } catch (IOException | DataParserException ex) {
      if(ex.getCause() instanceof AbortedException) {
        //If the pipeline was stopped, the amazon s3 client thread catches the interrupt and throws aborted exception
        //do not treat this as an error. Instead produce what ever you have and move one.

      } else {
        offset = S3Constants.MINUS_ONE;
        String exOffset;
        if (ex instanceof OverrunException) {
          exOffset = String.valueOf(((OverrunException) ex).getStreamOffset());
        } else {
          try {
            exOffset = (parser != null) ? parser.getOffset() : S3Constants.MINUS_ONE;
          } catch (IOException ex1) {
            LOG.warn("Could not get the object offset to report with error, reason: {}", ex1.toString(), ex);
            exOffset = S3Constants.MINUS_ONE;
          }
        }

        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            // we failed to produce a record, which leaves the input file in an unknown state. all we can do here is
            // throw an exception.
            throw new BadSpoolObjectException(s3Object.getKey(), exOffset, ex);
          case STOP_PIPELINE:
            getContext().reportError(Errors.S3_SPOOLDIR_03, s3Object.getKey(), exOffset, ex.toString(), ex);
            throw new StageException(Errors.S3_SPOOLDIR_03, s3Object.getKey(), exOffset, ex.toString(), ex);
          default:
            throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
              getContext().getOnErrorRecord(), ex));
        }
      }
    } finally {
      if (S3Constants.MINUS_ONE.equals(offset)) {
        if (parser != null) {
          try {
            parser.close();
            parser = null;
          } catch (IOException ex) {
            LOG.debug("Exception while closing parser : '{}'", ex.toString(), ex);
          }
        }
        if (object != null) {
          try {
            object.close();
            object = null;
          } catch (IOException ex) {
            LOG.debug("Exception while closing S3 object : '{}'", ex.toString(), ex);
          }
        }
      }
    }
    return offset;
  }
}
