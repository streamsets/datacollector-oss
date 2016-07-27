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
package com.streamsets.pipeline.stage.processor.dedup;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.queue.XEvictingQueue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DeDupProcessor extends RecordProcessor {
  private static final long MEMORY_USAGE_PER_HASH = 85;

  private final  int recordCountWindow;
  private final  int timeWindowSecs;
  private final  SelectFields compareFields;
  private final  List<String> fieldsToCompare;

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
  private Cache<HashCode, Object> hashCache;
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

    long estimatedMemory = MEMORY_USAGE_PER_HASH * recordCountWindow;
    long maxPipelineMemoryBytes = getContext().getPipelineMaxMemory() * 1000 * 1000;
    if (estimatedMemory > maxPipelineMemoryBytes) {
      issues.add(getContext().createConfigIssue(Groups.DE_DUP.name(), "recordCountWindow", Errors.DEDUP_03,
        recordCountWindow, estimatedMemory / (1000 * 1000), getContext().getPipelineMaxMemory()));
        //MiB to bytes conversion, use  1000 * 1000 instead of 1024 * 1024
    }
    if (issues.isEmpty()) {
      hasher = HashingUtil.getHasher(HashingUtil.HashType.MURMUR3_128);
      funnel = (compareFields == SelectFields.ALL_FIELDS) ? HashingUtil.getRecordFunnel(Collections.EMPTY_LIST) :
          HashingUtil.getRecordFunnel(fieldsToCompare);
      CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
      if (timeWindowSecs > 0) {
        cacheBuilder.expireAfterWrite(timeWindowSecs, TimeUnit.SECONDS);
      }
      hashCache = cacheBuilder.build(new CacheLoader<HashCode, Object>() {
        @Override
        public Object load(HashCode key) throws Exception {
          return VOID;
        }
      });
      hashBuffer = XEvictingQueue.create(recordCountWindow);
      hashAttrName = getInfo() + ".hash";
      uniqueLane = getContext().getOutputLanes().get(OutputStreams.UNIQUE.ordinal());
      duplicateLane = getContext().getOutputLanes().get(OutputStreams.DUPLICATE.ordinal());
    }
    return issues;
  }

  boolean duplicateCheck(Record record) {
    boolean dup = true;
    HashCode hash = hasher.hashObject(record, funnel);
    record.getHeader().setAttribute(hashAttrName, hash.toString());
    if (hashCache.getIfPresent(hash) == null) {
      hashCache.put(hash, VOID);
      HashCode evicted = hashBuffer.addAndGetEvicted(hash);
      if (evicted != null) {
        hashCache.invalidate(evicted);
      }
      dup = false;
    }
    return dup;
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    if (duplicateCheck(record)) {
      batchMaker.addRecord(record, duplicateLane);
    } else {
      batchMaker.addRecord(record, uniqueLane);
    }
  }

}
