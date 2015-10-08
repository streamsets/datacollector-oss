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
import com.streamsets.pipeline.api.base.BaseSource;
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
import java.util.concurrent.TimeUnit;

public class AmazonS3Source extends BaseSource {

  private final static Logger LOG = LoggerFactory.getLogger(AmazonS3Source.class);

  private static final String OFFSET_SEPARATOR = "::";
  private static final String MINUS_ONE = "-1";
  private static final String ZERO = "0";
  private static final long DEFAULT_FETCH_SIZE = 1 * 1024 * 1024;

  private final S3ConfigBean s3ConfigBean;
  private S3Spooler spooler;
  private S3ObjectSummary currentObject;
  private DataParser parser;
  private S3Object object;

  public AmazonS3Source(S3ConfigBean s3ConfigBean) {
    this.s3ConfigBean = s3ConfigBean;
  }

  public S3ObjectSummary getCurrentObject() {
    return currentObject;
  }

  public void setCurrentObject(S3ObjectSummary currentObject) {
    this.currentObject = currentObject;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    //init configuration
    s3ConfigBean.init(getContext(), issues);
    //preview settings
    if (getContext().isPreview()) {
      s3ConfigBean.basicConfig.maxWaitTime = 1000;
    }
    //init spooler
    if (issues.isEmpty()) {
      spooler = new S3Spooler(getContext(), s3ConfigBean);
      spooler.init();
    }

    return issues;
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(parser);
    s3ConfigBean.destroy();
    if(spooler != null) {
      spooler.destroy();
    }
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int batchSize = Math.min(s3ConfigBean.basicConfig.maxBatchSize, maxBatchSize);

    //parse offset string into S3Offset data structure
    S3Offset s3Offset = S3Offset.fromString(lastSourceOffset);

    spooler.postProcessOlderObjectIfNeeded(s3Offset);

    //check if we have an object to produce records from. Otherwise get from spooler.
    if (needToFetchNextObjectFromSpooler(s3Offset)) {
      s3Offset = fetchNextObjectFromSpooler(s3Offset);
      LOG.debug("Object '{}' with offset '{}' fetched from Spooler", s3Offset.getKey(), s3Offset.getOffset());
    } else {
      //check if the current object was modified between batches
      LOG.debug("Checking if Object '{}' has been modified between batches", getCurrentObject().getKey());
      if (!getCurrentObject().getETag().equals(s3Offset.geteTag())) {
        //send the current object to error archive and get next object from spooler
        LOG.debug("Object '{}' has been modified between batches. Sending the object to error",
          getCurrentObject().getKey());
        try {
          spooler.handleCurrentObjectAsError();
        } catch (AmazonClientException e) {
          throw new StageException(Errors.S3_SPOOLDIR_24, e.toString(), e);
        }
        s3Offset = fetchNextObjectFromSpooler(s3Offset);
      }
    }

    if (getCurrentObject() != null) {
      try {
        // we ask for a batch from the currentObject starting at offset
        s3Offset.setOffset(produce(getCurrentObject(), s3Offset.getOffset(), batchSize, batchMaker));
      } catch (BadSpoolObjectException ex) {
        LOG.error(Errors.S3_SPOOLDIR_01.getMessage(), ex.getObject(), ex.getPos(), ex.toString(), ex);
        getContext().reportError(Errors.S3_SPOOLDIR_01, ex.getObject(), ex.getPos(), ex.toString());
        try {
          spooler.handleCurrentObjectAsError();
        } catch (AmazonClientException e) {
          throw new StageException(Errors.S3_SPOOLDIR_24, e.toString(), e);
        }
        // we set the offset to -1 to indicate we are done with the current object and we should fetch a new one
        // from the spooler
        s3Offset.setOffset(MINUS_ONE);
      }
    }
    return s3Offset.toString();
  }

  private S3Offset fetchNextObjectFromSpooler(S3Offset s3Offset) throws StageException {
    setCurrentObject(null);
    try {
      //The next object found in queue is mostly eligible since we process objects in chronological order.

      //However after processing a few files, if the configuration is changed [say relax the prefix] and an older file
      //gets selected for processing, it must be ignored.
      S3ObjectSummary nextAvailObj = null;
      do {
        if (nextAvailObj != null) {
          LOG.warn("Ignoring object '{}' in spool directory as is lesser than offset object '{}'",
            nextAvailObj.getKey(), s3Offset.getKey());
        }
        nextAvailObj = spooler.poolForObject(s3Offset, s3ConfigBean.basicConfig.maxWaitTime, TimeUnit.MILLISECONDS);
      } while (!isEligible(nextAvailObj, s3Offset));

      if (nextAvailObj == null) {
        // no object to process
        LOG.debug("No new object available in spool directory after '{}' secs, producing empty batch",
          s3ConfigBean.basicConfig.maxWaitTime/1000);
      } else {
        setCurrentObject(nextAvailObj);

        // if the current offset object is null or the object returned by the spooler is greater than the current offset
        // object we take the object returned by the spooler as the new object and set the offset to zero.
        // if not, it means the spooler returned us the current object, we just keep processing it from the last
        // offset we processed (known via offset tracking)
        if (s3Offset.getKey() == null ||
          isLaterThan(nextAvailObj.getKey(), nextAvailObj.getLastModified().getTime(), s3Offset.getKey(),
            Long.parseLong(s3Offset.getTimestamp()))) {
          s3Offset = new S3Offset(getCurrentObject().getKey(), ZERO, getCurrentObject().getETag(),
            String.valueOf(getCurrentObject().getLastModified().getTime()));
        }
      }
    } catch (InterruptedException ex) {
      // the spooler was interrupted while waiting for an object, we log and return, the pipeline agent will invoke us
      // again to wait for an object again
      LOG.warn("Pooling interrupted");
    } catch (AmazonClientException e) {
      throw new StageException(Errors.S3_SPOOLDIR_23, e.toString(), e);
    }
    return s3Offset;
  }

  public String produce(S3ObjectSummary s3Object, String offset, int maxBatchSize, BatchMaker batchMaker) throws StageException,
    BadSpoolObjectException {
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
            offset = MINUS_ONE;
            break;
          }
        } catch (ObjectLengthException ex) {
          String exOffset = offset;
          offset = MINUS_ONE;
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().reportError(Errors.S3_SPOOLDIR_02, s3Object.getKey(), exOffset);
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
        offset = MINUS_ONE;
        String exOffset;
        if (ex instanceof OverrunException) {
          exOffset = String.valueOf(((OverrunException) ex).getStreamOffset());
        } else {
          try {
            exOffset = (parser != null) ? parser.getOffset() : MINUS_ONE;
          } catch (IOException ex1) {
            LOG.warn("Could not get the object offset to report with error, reason: {}", ex1.toString(), ex);
            exOffset = MINUS_ONE;
          }
        }

        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            throw new BadSpoolObjectException(s3Object.getKey(), exOffset, ex);
          case STOP_PIPELINE:
            getContext().reportError(Errors.S3_SPOOLDIR_03, s3Object.getKey(), exOffset, ex.toString());
            throw new StageException(Errors.S3_SPOOLDIR_03, s3Object.getKey(), exOffset, ex.toString(), ex);
          default:
            throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
              getContext().getOnErrorRecord(), ex));
        }
      }
    } finally {
      if (MINUS_ONE.equals(offset)) {
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

  private boolean needToFetchNextObjectFromSpooler(S3Offset s3Offset) {
    return
      // we don't have an object half way processed in the current agent execution
      getCurrentObject() == null ||
      // we don't have an object half way processed from a previous agent execution via offset tracking
      s3Offset.getKey() == null ||
      // the current object has been fully processed
      MINUS_ONE.equals(s3Offset.getOffset());
  }

  private boolean isEligible(S3ObjectSummary nextAvailObj, S3Offset s3Offset) {
    return (nextAvailObj == null) ||
      (nextAvailObj.getLastModified().getTime() >= Long.parseLong(s3Offset.getTimestamp()));
  }

  private boolean isLaterThan(String nextKey, long nextTimeStamp, String originalKey, long originalTimestamp) {
    return (nextTimeStamp > originalTimestamp) ||
      (nextTimeStamp == originalTimestamp && nextKey.compareTo(originalKey) > 0);
  }

  static class S3Offset {
    private final String key;
    private final String eTag;
    private String offset;
    private final String timestamp;

    public S3Offset(String key, String offset, String eTag, String timestamp) {
      this.key = key;
      this.offset = offset;
      this.eTag = eTag;
      this.timestamp = timestamp;
    }

    public String getKey() {
      return key;
    }

    public String geteTag() {
      return eTag;
    }

    public String getOffset() {
      return offset;
    }

    public String getTimestamp() {
      return timestamp;
    }

    public void setOffset(String offset) {
      this.offset = offset;
    }

    @Override
    public String toString() {
      return key + OFFSET_SEPARATOR + offset + OFFSET_SEPARATOR + eTag + OFFSET_SEPARATOR + timestamp;
    }

    public static S3Offset fromString(String lastSourceOffset) throws StageException {
      if (lastSourceOffset != null) {
        String[] split = lastSourceOffset.split(OFFSET_SEPARATOR);
        if (split.length == 4) {
          return new S3Offset(split[0], split[1], split[2], split[3]);
        } else {
          throw new StageException(Errors.S3_SPOOLDIR_21, lastSourceOffset);
        }
      }
      return new S3Offset(null, ZERO, null, ZERO);
    }
  }

}
