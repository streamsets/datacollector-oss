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

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class AmazonS3SourceImpl extends AbstractAmazonS3Source implements AmazonS3Source {
  private volatile Map<Integer, S3Offset> offsetsMap;
  private volatile Queue<S3Offset> orphanThreads;
  private volatile S3Offset latestOffset = new S3Offset(null, S3Constants.ZERO, null, S3Constants.ZERO);

  private PushSource.Context context;

  AmazonS3SourceImpl(S3ConfigBean s3ConfigBean) {
    super(s3ConfigBean);
    offsetsMap = Collections.synchronizedMap(new LinkedHashMap<>());
    orphanThreads = new LinkedList<>();
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

  private void createInitialOffsetsMap(Map<String, String> lastSourceOffset) throws StageException {
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
      latestOffset = s3Offset;
      threadCount++;
    }
  }

  private List<S3Offset> orderOffsets(List<S3Offset> unorderedListOfOffsets) {
    ObjectOrdering objectOrdering = s3ConfigBean.s3FileConfig.objectOrdering;
    switch (objectOrdering) {
      case TIMESTAMP:
        Collections.sort(unorderedListOfOffsets, new TimeStampComparator());
        break;
      case LEXICOGRAPHICAL:
        Collections.sort(unorderedListOfOffsets, new LexicographicalComparator());
        break;
      default:
        throw new IllegalArgumentException("Unknown ordering: " + objectOrdering.getLabel());
    }
    return unorderedListOfOffsets;
  }

  public class TimeStampComparator implements Comparator<S3Offset> {
    @Override
    public int compare(S3Offset o1, S3Offset o2) {
      return o1.getTimestamp().compareTo(o2.getTimestamp());
    }
  }

  public class LexicographicalComparator implements Comparator<S3Offset> {
    @Override
    public int compare(S3Offset o1, S3Offset o2) {
      return o1.getKey().compareTo(o2.getKey());
    }
  }

  @Override
  public void updateOffset(Integer key, S3Offset value) {
    if (value.getKey() != null) {

      if (!isKeyAlreadyInMap(value.getKey())) {
        offsetsMap.put(key, value);
        context.commitOffset(String.valueOf(key), value.toString());
        updateLatestOffset(value);
        return;
      }

      S3Offset offset = getOffsetFromGivenKey(value.getKey());
      if (!offset.getOffset().equals(S3Constants.MINUS_ONE)) {
        if (value.getOffset().equals(S3Constants.MINUS_ONE)) {
          offsetsMap.put(key, value);
          context.commitOffset(String.valueOf(key), value.toString());
          updateLatestOffset(value);
        } else if (Integer.valueOf(value.getOffset()) > Integer.valueOf(offset.getOffset())) {
          offsetsMap.put(key, value);
          context.commitOffset(String.valueOf(key), value.toString());
          updateLatestOffset(value);
        }
      }
    }
  }

  private void updateLatestOffset(S3Offset offset) {
    if (latestOffset.getKey().equals(offset.getKey()) && !latestOffset.getOffset().equals(offset.getOffset())) {
      latestOffset = offset;
    }
  }

  @Override
  public void addNewLatestOffset(S3Offset offset) {
    latestOffset = offset;
  }

  private S3Offset getOffsetFromGivenKey(String key) {
    for (S3Offset offset : offsetsMap.values()) {
      if (offset.getKey() != null && offset.getKey().equals(key)) {
        return offset;
      }
    }
    return null;
  }

  private boolean isKeyAlreadyInMap(String key) {
    boolean exists = true;
    if (getOffsetFromGivenKey(key) == null) {
      exists = false;
    }
    return exists;
  }

  @Override
  public S3Offset getOffset(Integer key) {
    if (offsetsMap.containsKey(key) && offsetsMap.get(key).getOffset().equals(S3Constants.MINUS_ONE)) {
      //If current record is finished and we prefer to get one of the orphans
      if (!orphanThreads.isEmpty()) {
        offsetsMap.remove(key);
        S3Offset value = orphanThreads.poll();
        value = value != null ? value : new S3Offset(null, S3Constants.ZERO, null, S3Constants.ZERO);
        offsetsMap.put(key, value);
      }
    } else if (offsetsMap.containsKey(key)) {
      return offsetsMap.get(key);
    } else {
      offsetsMap.put(key, new S3Offset(null, S3Constants.ZERO, null, S3Constants.ZERO));
    }
    return offsetsMap.get(key);
  }

  @Override
  public S3Offset getLatestOffset() {
    return latestOffset;
  }
}
