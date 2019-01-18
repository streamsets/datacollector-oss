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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class AmazonS3SourceImpl extends AbstractAmazonS3Source implements AmazonS3Source {
  volatile Map<Integer, S3Offset> offsetsMap;
  volatile Queue<S3Offset> orphanThreads;
  private AtomicBoolean noMoreDataEventSent;

  private AtomicLong noMoreDataRecordCount;
  private AtomicLong noMoreDataErrorCount;
  private AtomicLong noMoreDataFileCount;

  private PushSource.Context context;

  AmazonS3SourceImpl(S3ConfigBean s3ConfigBean) {
    super(s3ConfigBean);
    offsetsMap = new ConcurrentHashMap<>();
    orphanThreads = new LinkedList<>();
    noMoreDataEventSent = new AtomicBoolean(false);
    noMoreDataRecordCount = new AtomicLong();
    noMoreDataErrorCount = new AtomicLong();
    noMoreDataFileCount = new AtomicLong();
  }

  @Override
  public Map<Integer, S3Offset> handleOffset(Map<String, String> lastSourceOffset, PushSource.Context context)
      throws StageException {
    this.context = context;
    int threadCount = 0;

    if (lastSourceOffset.containsKey(Source.POLL_SOURCE_OFFSET_KEY)) {
      //This code will be executed only the first time after moving from singlethread to multithread
      offsetsMap.put(threadCount, S3Offset.fromString(lastSourceOffset.get(Source.POLL_SOURCE_OFFSET_KEY)));
    } else {
      createInitialOffsetsMap(lastSourceOffset);
    }
    return offsetsMap;
  }

  @VisibleForTesting
  void createInitialOffsetsMap(Map<String, String> lastSourceOffset) throws StageException {
    List<S3Offset> unorderedListOfOffsets = new ArrayList<>();
    for (String offset : lastSourceOffset.values()) {
      unorderedListOfOffsets.add(S3Offset.fromString(offset));
    }

    List<S3Offset> orderedListOfOffsets = orderOffsets(unorderedListOfOffsets);
    int threadCount = 0;

    for (S3Offset s3Offset : orderedListOfOffsets) {
      // All the offsets are added to offsetsMap and the threads are consuming from there. If someone stop the
      // pipeline and start it again with less threads, there are some that will not be resumed with a new thread, in
      // this case it will be also added to orphanThreads, from where will be consumed before polling S3 for new objects
      if (threadCount <= s3ConfigBean.numberOfThreads) {
        offsetsMap.put(threadCount, s3Offset);
      } else {
        offsetsMap.put(threadCount, s3Offset);
        orphanThreads.add(s3Offset);
      }
      threadCount++;
    }
  }

  @VisibleForTesting
  List<S3Offset> orderOffsets(List<S3Offset> offsetsList) {
    ObjectOrdering objectOrdering = s3ConfigBean.s3FileConfig.objectOrdering;
    switch (objectOrdering) {
      case TIMESTAMP:
        offsetsList.sort(Comparator.comparing(S3Offset::getTimestamp));
        break;
      case LEXICOGRAPHICAL:
        offsetsList.sort(Comparator.comparing(S3Offset::getKey));
        break;
      default:
        throw new IllegalArgumentException("Unknown ordering: " + objectOrdering.getLabel());
    }
    return offsetsList;
  }

  @Override
  public void updateOffset(Integer runnerId, S3Offset s3Offset) {
    if (s3Offset.getKey() != null) {

      if (!isKeyAlreadyInMap(s3Offset.getKey())) {
        offsetsMap.put(runnerId, s3Offset);
        context.commitOffset(String.valueOf(runnerId), s3Offset.toString());
        return;
      }

      S3Offset offset = getOffsetFromGivenKey(s3Offset.getKey());
      if (!offset.getOffset().equals(S3Constants.MINUS_ONE)) {
        if (s3Offset.getOffset().equals(S3Constants.MINUS_ONE)) {
          offsetsMap.put(runnerId, s3Offset);
          context.commitOffset(String.valueOf(runnerId), s3Offset.toString());
        } else if (Integer.valueOf(s3Offset.getOffset()) > Integer.valueOf(offset.getOffset())) {
          offsetsMap.put(runnerId, s3Offset);
          context.commitOffset(String.valueOf(runnerId), s3Offset.toString());
        }
      }
    }
  }

  @VisibleForTesting
  S3Offset getOffsetFromGivenKey(String key) {
    for (S3Offset offset : offsetsMap.values()) {
      if (offset.getKey() != null && offset.getKey().equals(key)) {
        return offset;
      }
    }
    return null;
  }

  @VisibleForTesting
  boolean isKeyAlreadyInMap(String key) {
    boolean exists = true;
    if (getOffsetFromGivenKey(key) == null) {
      exists = false;
    }
    return exists;
  }

  @Override
  public S3Offset getOffset(Integer runnerId) {
    // If the current value for that runnerId is MINUS_ONE and we have any orphanThread we will get the next value
    // from there, if not, we will create a new empty offset
    S3Offset offset;
    S3Offset currentOffset = offsetsMap.get(runnerId);
    if (currentOffset != null && currentOffset.getOffset().equals(S3Constants.MINUS_ONE) && !orphanThreads.isEmpty()) {
      offset = orphanThreads.poll();
      offset = offset != null ? offset : new S3Offset(S3Constants.EMPTY,
          S3Constants.ZERO,
          S3Constants.EMPTY,
          S3Constants.ZERO
      );
      offsetsMap.put(runnerId, offset);
    } else {
      offset = offsetsMap.computeIfAbsent(
          runnerId,
          k -> new S3Offset(S3Constants.EMPTY, S3Constants.ZERO, S3Constants.EMPTY, S3Constants.ZERO)
      );
    }
    return offset;
  }
  @Override
  public S3Offset getLatestOffset() {
    List<S3Offset> orderedOffsets = orderOffsets(new ArrayList<>(offsetsMap.values()));
    return orderedOffsets.get(orderedOffsets.size() - 1);
  }

  @Override
  public long incrementNoMoreDataRecordCount() {
    return noMoreDataRecordCount.incrementAndGet();
  }

  @Override
  public long incrementNoMoreDataErrorCount() {
    return noMoreDataErrorCount.incrementAndGet();
  }

  @Override
  public long incrementNoMoreDataFileCount() {
    return noMoreDataFileCount.incrementAndGet();
  }

  @Override
  public void sendNoMoreDataEvent(BatchContext batchContext) {
    if (allFilesAreFinished() && !noMoreDataEventSent.getAndSet(true)) {
      S3Events.NO_MORE_DATA.create(context, batchContext)
                           .with("record-count", noMoreDataRecordCount.get())
                           .with("error-count", noMoreDataErrorCount.get())
                           .with("file-count", noMoreDataFileCount.get())
                           .createAndSend();
      noMoreDataRecordCount.set(0);
      noMoreDataErrorCount.set(0);
      noMoreDataFileCount.set(0);
    }
  }

  @VisibleForTesting
  boolean allFilesAreFinished() {
    boolean filesFinished = true;
    for (S3Offset s3Offset : offsetsMap.values()) {
      filesFinished = s3Offset.getOffset().equals(S3Constants.MINUS_ONE);
      if (!filesFinished) {
        break;
      }
    }
    return filesFinished;
  }
}
