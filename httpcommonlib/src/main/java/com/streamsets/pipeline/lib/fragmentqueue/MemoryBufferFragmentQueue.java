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
package com.streamsets.pipeline.lib.fragmentqueue;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryBufferFragmentQueue implements FragmentQueue {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryBufferFragmentQueue.class);

  private final int maxMemQueueFragments;
  private final BlockingQueue<byte[]> dataQueue;
  private final FragmentQueue fragmentQueue;
  private volatile boolean running;
  private Thread writerThread;
  private AtomicInteger lostFragments;
  private volatile long lastMemoryPoll;

  public MemoryBufferFragmentQueue(int maxMemQueueFragments, FragmentQueue fragmentQueue) {
    this.maxMemQueueFragments = maxMemQueueFragments;
    this.fragmentQueue = fragmentQueue;
    this.dataQueue = new ArrayBlockingQueue<byte[]>(maxMemQueueFragments);
  }

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = fragmentQueue.init(context);
    if (issues.isEmpty()) {
      running = true;
      writerThread = new Thread(getWriterRunnable());
      writerThread.setName("MemoryBufferFragmentQueueWriter");
      writerThread.setDaemon(true);
      writerThread.start();
    }
    lostFragments = new AtomicInteger(0);
    return issues;
  }

  @VisibleForTesting
  Runnable getWriterRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        List<byte[]> list = new ArrayList<>(1);
        list.add(null);
        try {
          while (running) {
            try {
              byte[] fragment = dataQueue.poll(100, TimeUnit.MILLISECONDS);
              if (fragment != null) {
                list.set(0, fragment);
                fragmentQueue.write(list);
              }
            } catch (InterruptedException ex) {
              //NOP
            }
            lastMemoryPoll = System.currentTimeMillis();
          }
        } catch (Exception ex) {
          // LOG WARN
        }
      }
    };
  }

  @VisibleForTesting
  long getLastMemoryPoll() {
    return lastMemoryPoll;
  }

  @Override
  public void destroy() {
    running = false;
    writerThread.interrupt();
    fragmentQueue.destroy();
  }

  @Override
  public int getMaxFragmentSizeKB() {
    return fragmentQueue.getMaxFragmentSizeKB();
  }

  @Override
  public void write(List<byte[]> fragments) throws IOException {
    int lost = 0;
    for (byte[] fragment : fragments) {
      if (!dataQueue.offer(fragment)) {
        lost++;
      }
    }
    if (lost > 0) {
      lostFragments.addAndGet(lost);
      LOG.warn("Lost '{}' fragments, memory queue full at '{}'", lost, maxMemQueueFragments);
    }
  }

  @Override
  public int getLostFragmentsCountAndReset() {
    return lostFragments.getAndSet(0) + fragmentQueue.getLostFragmentsCountAndReset();
  }

  @Override
  public List<byte[]> poll(int maxFragments) throws IOException {
    return fragmentQueue.poll(maxFragments);
  }

  public List<byte[]> poll(int maxFragments, long waitTimeMillis) throws IOException, InterruptedException {
    return fragmentQueue.poll(maxFragments, waitTimeMillis);
  }

}
