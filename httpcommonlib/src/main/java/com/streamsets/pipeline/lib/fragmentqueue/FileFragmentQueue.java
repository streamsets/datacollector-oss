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
import com.squareup.tape.QueueFile;
import com.streamsets.pipeline.api.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FileFragmentQueue implements FragmentQueue {
  private static final Logger LOG = LoggerFactory.getLogger(FileFragmentQueue.class);

  private File file;
  private QueueFile queueFile;
  private AtomicLong queueFileSize = new AtomicLong(0);
  private AtomicInteger lostFragments;
  private long maxQueueFileSize;

  public FileFragmentQueue(long maxFileSizeMB) {
    this.maxQueueFileSize = maxFileSizeMB * 1000 * 1000;
  }

  private File getFile() {
    return file;
  }

  private QueueFile getQueueFile() {
    return queueFile;
  }

  private AtomicLong getQueueFileSize() {
    return queueFileSize;
  }

  @VisibleForTesting
  long getMaxQueueFileSize() {
    return maxQueueFileSize;
  }

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    try {
      file = File.createTempFile("sdc-fragments", ".queueFile");
      if (!getFile().delete()) {
        issues.add(context.createConfigIssue("", "", Errors.FRAGMENT_CACHE_WRITER_00, getFile().getAbsolutePath()));
      }
      queueFile = new QueueFile(getFile());
      queueFileSize = new AtomicLong(0);
    } catch (IOException ex) {
      issues.add(context.createConfigIssue("", "", Errors.FRAGMENT_CACHE_WRITER_01, ex.toString()));
    }
    lostFragments = new AtomicInteger(0);
    return issues;
  }

  @Override
  public void destroy() {
    if (getQueueFile() != null) {
      try {
        getQueueFile().close();
        if (!file.delete()) {
          LOG.warn("Could not delete queue file '{}' on destroy", getFile().getAbsolutePath());
        }
      } catch (IOException ex) {
        LOG.warn("Error deleting queue file '{}' on destroy: {}", getFile().getAbsolutePath(), ex.toString(), ex);
      }
    }
  }

  @Override
  public int getMaxFragmentSizeKB() {
    return (int) (maxQueueFileSize / 1000);
  }

  @Override
  public void write(List<byte[]> fragments) throws IOException {
    int lost = 0;
    for (byte[] fragment : fragments) {
      if (getQueueFileSize().get() + fragment.length < getMaxQueueFileSize()) {
        getQueueFile().add(fragment);
        getQueueFileSize().addAndGet(fragment.length);
      } else {
        lost++;
      }
    }
    if (lost > 0) {
      lostFragments.addAndGet(lost);
      LOG.warn("Lost '{}' fragments, file queue full at '{}MB'", lost, getMaxQueueFileSize() / 1000 / 1000);
    }
  }

  @Override
  public int getLostFragmentsCountAndReset() {
    return lostFragments.getAndSet(0);
  }

  @Override
  public List<byte[]> poll(int maxFragments) throws IOException {
    List<byte[]> fragments = null;
    byte[] data;
    do {
      data = getQueueFile().peek();
      if (data != null) {
        getQueueFile().remove();
        if (fragments == null) {
          fragments = new ArrayList<>();
        }
        fragments.add(data);
      }
    } while (data != null && fragments.size() < maxFragments);
    return fragments;
  }

  public List<byte[]> poll(int maxFragments, long waitTimeMillis) throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    List<byte[]> fragments = poll(maxFragments);
    while (fragments == null && System.currentTimeMillis() - start <= waitTimeMillis) {
      Thread.sleep(50);
      fragments = poll(maxFragments);
    }
    return fragments;
  }
}
