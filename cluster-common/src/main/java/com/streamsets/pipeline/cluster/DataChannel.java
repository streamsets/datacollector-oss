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
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.impl.OffsetAndResult;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Implements a uni-directional data flow from producer to consumer.
 */
public class DataChannel {

  private final BlockingQueue<OffsetAndResult<Map.Entry>> dataQueue = new ArrayBlockingQueue<>(1);

  public boolean offer(OffsetAndResult<Map.Entry> batch, long timeout, TimeUnit unit) throws InterruptedException {
    return dataQueue.offer(batch, timeout, unit);
  }

  public OffsetAndResult<Map.Entry> take(long timeout, TimeUnit unit) throws InterruptedException {
    return dataQueue.poll(timeout, unit);
  }
}
