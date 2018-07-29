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
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.util.AntPathMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class S3Spooler {

  private static final Logger LOG = LoggerFactory.getLogger(S3Spooler.class);

  private final Source.Context context;
  private final S3ConfigBean s3ConfigBean;
  private final AmazonS3 s3Client;
  private AntPathMatcher pathMatcher;

  public S3Spooler(Source.Context context, S3ConfigBean s3ConfigBean) {
    this.context = context;
    this.s3ConfigBean = s3ConfigBean;
    this.s3Client = s3ConfigBean.s3Config.getS3Client();
  }

  private S3ObjectSummary currentObject;
  private ArrayBlockingQueue<S3ObjectSummary> objectQueue;
  private Meter spoolQueueMeter;

  public void init() {
    try {
      objectQueue = new ArrayBlockingQueue<>(s3ConfigBean.s3FileConfig.poolSize);
      spoolQueueMeter = context.createMeter("spoolQueue");
      pathMatcher = new AntPathMatcher(s3ConfigBean.s3Config.delimiter);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void destroy() {
    if(objectQueue != null) {
      objectQueue.clear();
      objectQueue = null;
    }
  }

  S3ObjectSummary findAndQueueObjects(AmazonS3Source.S3Offset s3offset, boolean checkCurrent)
    throws AmazonClientException {
    List<S3ObjectSummary> s3ObjectSummaries;
    ObjectOrdering objectOrdering = s3ConfigBean.s3FileConfig.objectOrdering;
    switch (objectOrdering) {
      case TIMESTAMP:
        s3ObjectSummaries = AmazonS3Util.listObjectsChronologically(
            s3Client,
            s3ConfigBean,
            pathMatcher,
            s3offset,
            objectQueue.remainingCapacity()
        );
        break;
      case LEXICOGRAPHICAL:
        s3ObjectSummaries = AmazonS3Util.listObjectsLexicographically(
            s3Client,
            s3ConfigBean,
            pathMatcher,
            s3offset,
            objectQueue.remainingCapacity()
        );
        break;
      default:
        throw new IllegalArgumentException("Unknown ordering: " + objectOrdering.getLabel());
    }
    for (S3ObjectSummary objectSummary : s3ObjectSummaries) {
      addObjectToQueue(objectSummary, checkCurrent);
    }
    spoolQueueMeter.mark(objectQueue.size());
    LOG.debug("Found '{}' files", objectQueue.size());
    return (s3ObjectSummaries.isEmpty()) ? null : s3ObjectSummaries.get(s3ObjectSummaries.size() - 1);
  }

  void addObjectToQueue(S3ObjectSummary objectSummary, boolean checkCurrent) {
    Preconditions.checkNotNull(objectSummary, "file cannot be null");
    if (checkCurrent) {
      Preconditions.checkState(currentObject == null ||
        currentObject.getLastModified().compareTo(objectSummary.getLastModified()) < 0);
    }
    if (!objectQueue.contains(objectSummary)) {
      objectQueue.add(objectSummary);
      spoolQueueMeter.mark(objectQueue.size());
    } else {
      LOG.warn("Object '{}' already in queue, ignoring", objectSummary.getKey());
    }
  }

  public S3ObjectSummary poolForObject(AmazonS3Source.S3Offset s3Offset, long wait, TimeUnit timeUnit)
    throws InterruptedException, AmazonClientException {
    Preconditions.checkArgument(wait >= 0, "wait must be zero or greater");
    Preconditions.checkNotNull(timeUnit, "timeUnit cannot be null");

    if(objectQueue.isEmpty()) {
      findAndQueueObjects(s3Offset, false);
    }

    S3ObjectSummary next = null;
    try {
      LOG.debug("Polling for file, waiting '{}' ms", TimeUnit.MILLISECONDS.convert(wait, timeUnit));
      next = objectQueue.poll(wait, timeUnit);
    } catch (InterruptedException ex) {
      next = null;
    } finally {
      LOG.debug("Polling for file returned '{}'", next);
      if (next != null) {
        currentObject = next;
      }
    }
    return next;
  }

  void postProcessOrErrorHandle(String postProcessObjectKey, PostProcessingOptions postProcessing, String postProcessBucket,
                                String postProcessFolder, S3ArchivingOption archivingOption) {
    switch (postProcessing) {
      case NONE:
        break;
      case DELETE:
        delete(postProcessObjectKey);
        break;
      case ARCHIVE:
        archive(postProcessObjectKey, postProcessBucket, postProcessFolder, archivingOption);
        break;
      default:
        throw new IllegalStateException("Invalid post processing option : " +
          s3ConfigBean.postProcessingConfig.postProcessing.name());
    }
  }

  private void archive(String postProcessObjectKey, String postProcessBucket, String postProcessFolder,
                       S3ArchivingOption archivingOption) {
    boolean isMove = true;
    String destBucket = s3ConfigBean.s3Config.bucket;
    switch (archivingOption) {
      case MOVE_TO_PREFIX:
        break;
      case MOVE_TO_BUCKET:
        destBucket = postProcessBucket;
        break;
      case COPY_TO_PREFIX:
        isMove = false;
        break;
      case COPY_TO_BUCKET:
        isMove = false;
        destBucket = postProcessBucket;
        break;
      default:
        throw new IllegalStateException("Invalid Archive option : " + archivingOption.name());
    }
    String srcObjKey = postProcessObjectKey.substring(
      postProcessObjectKey.lastIndexOf(s3ConfigBean.s3Config.delimiter) + 1);
    String destKey = postProcessFolder + srcObjKey;
    AmazonS3Util.copy(s3Client, s3ConfigBean.s3Config.bucket, postProcessObjectKey, destBucket, destKey, isMove);
  }

  private void delete(String postProcessObjectKey) {
    LOG.debug("Deleting previous file '{}'", postProcessObjectKey);
    s3Client.deleteObject(s3ConfigBean.s3Config.bucket, postProcessObjectKey);
  }

  public void handleCurrentObjectAsError() {
    if (currentObject != null) {
      //Move to error prefix only if the error bucket and prefix is specified and is different from
      //source bucket and prefix
      Utils.checkNotNull(s3ConfigBean.errorConfig, "s3ConfigBean.errorConfig");
      postProcessOrErrorHandle(currentObject.getKey(), s3ConfigBean.errorConfig.errorHandlingOption,
          s3ConfigBean.errorConfig.errorBucket, s3ConfigBean.errorConfig.errorPrefix,
          s3ConfigBean.errorConfig.archivingOption);
      currentObject = null;
    } else {
      LOG.debug("Current object is null");
    }
  }

  public void postProcessOlderObjectIfNeeded(AmazonS3Source.S3Offset s3Offset) {
    //If sdc was shutdown after reading an object but before post processing it, handle it now.

    //The scenario is detected as follows:
    //  1. the current key must not be null
    //  2. offset must be -1
    //  3. An object with same key must exist in s3
    //  4. The timestamp of the object ins3 must be same as that of the timestamp in offset [It is possible that one
    //    uploads another object with the same name. We can avoid post processing it without producing records by
    //    comparing the timestamp on that object

    if(s3Offset.getKey() != null &&
      "-1".equals(s3Offset.getOffset())) {
      //conditions 1, 2 are met. Check for 3 and 4.
      S3ObjectSummary objectSummary = AmazonS3Util.getObjectSummary(s3Client, s3ConfigBean.s3Config.bucket, s3Offset.getKey());
      if(objectSummary != null &&
        objectSummary.getLastModified().compareTo(new Date(Long.parseLong(s3Offset.getTimestamp()))) == 0) {
        postProcessOrErrorHandle(s3Offset.getKey(), s3ConfigBean.postProcessingConfig.postProcessing,
          s3ConfigBean.postProcessingConfig.postProcessBucket, s3ConfigBean.postProcessingConfig.postProcessPrefix,
          s3ConfigBean.postProcessingConfig.archivingOption);
      }
    }
    currentObject = null;
  }
}
