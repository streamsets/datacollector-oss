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
package com.streamsets.pipeline.stage.processor.dedup;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.queue.XEvictingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class DeDupProcessor extends RecordProcessor {
  private static final String CACHE_KEY = "cache";
  private static final Logger LOG = LoggerFactory.getLogger(DeDupProcessor.class);

  private final  int recordCountWindow;
  private final  int timeWindowSecs;
  private final  SelectFields compareFields;
  private final  List<String> fieldsToCompare;
  private CacheCleaner cacheCleaner;

  public DeDupProcessor(int recordCountWindow, int timeWindowSecs,
      SelectFields compareFields, List<String> fieldsToCompare) {
    this.recordCountWindow = recordCountWindow;
    this.timeWindowSecs = timeWindowSecs;
    this.compareFields = compareFields;
    this.fieldsToCompare = fieldsToCompare;
  }

  private static final Object VOID = new Object();

  private HashFunction hasher;
  private HashingUtil.RecordFunnel funnel;
  private Cache<HashCode, HashCode> hashCache;
  private XEvictingQueue<HashCode> hashBuffer;
  private String uniqueLane;
  private String duplicateLane;

  private String hashAttrName;

  @Override
  @SuppressWarnings("unchecked")
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (recordCountWindow <= 0) {
      issues.add(getContext().createConfigIssue(Groups.DE_DUP.name(), "recordCountWindow", Errors.DEDUP_00,
                                                recordCountWindow));
    }
    if (timeWindowSecs < 0) {
      issues.add(getContext().createConfigIssue(Groups.DE_DUP.name(), "timeWindowSecs", Errors.DEDUP_01,
                                                timeWindowSecs));
    }
    if (compareFields == SelectFields.SPECIFIED_FIELDS && fieldsToCompare.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.DE_DUP.name(), "compareFields", Errors.DEDUP_02));
    }

    if (issues.isEmpty()) {
      hasher = HashingUtil.getHasher(HashingUtil.HashType.MURMUR3_128);

      funnel = (compareFields == SelectFields.ALL_FIELDS) ? HashingUtil.getRecordFunnel(
          Collections.EMPTY_LIST,
          false,
          true,
          '\u0000'
      ) : HashingUtil.getRecordFunnel(fieldsToCompare, false, true, '\u0000');

      Map<String, Object> runnerSharedMap = getContext().getStageRunnerSharedMap();
      synchronized (runnerSharedMap) {
        if(!runnerSharedMap.containsKey(CACHE_KEY)) {
          CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
          if (timeWindowSecs > 0) {
            cacheBuilder.expireAfterWrite(timeWindowSecs, TimeUnit.SECONDS);
          }
          if(LOG.isDebugEnabled()) {
            cacheBuilder.recordStats();
          }
          hashCache = cacheBuilder.build();

          runnerSharedMap.put(CACHE_KEY, hashCache);
        } else {
          hashCache = (Cache<HashCode, HashCode>) runnerSharedMap.get(CACHE_KEY);
        }
      }
      cacheCleaner = new CacheCleaner(hashCache, "DeDupProcessor", 10 * 60 * 1000);

      hashBuffer = XEvictingQueue.create(recordCountWindow);
      hashAttrName = getInfo() + ".hash";
      uniqueLane = getContext().getOutputLanes().get(OutputStreams.UNIQUE.ordinal());
      duplicateLane = getContext().getOutputLanes().get(OutputStreams.DUPLICATE.ordinal());
    }
    return issues;
  }

  boolean duplicateCheck(Record record) throws ExecutionException {
    HashCode hash = hasher.hashObject(record, funnel);
    record.getHeader().setAttribute(hashAttrName, hash.toString());

    HashCode hashInstance = hashCache.get(hash, () -> hash);
    // We are riding on the fact that if the instance is the same we just added and it is not a dup
    boolean dup = hashInstance != hash;

    // Eviction is done in async manner - e.g. around the eviction time, we can possibly not issue a record because
    // we still think that it's a duplicate when in facts it's not.
    if (!dup) {
      HashCode evicted = hashBuffer.addAndGetEvicted(hash);
      if (evicted != null) {
        hashCache.invalidate(evicted);
      }
    }

    return dup;
  }

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
    }
    super.process(batch, batchMaker);
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    try {
      if (duplicateCheck(record)) {
        batchMaker.addRecord(record, duplicateLane);
      } else {
        batchMaker.addRecord(record, uniqueLane);
      }
    } catch (IllegalArgumentException|ExecutionException e) {
      LOG.error("Error processing Record", e);
      throw new OnRecordErrorException(Errors.DEDUP_04, e.toString());
    }
  }

}
