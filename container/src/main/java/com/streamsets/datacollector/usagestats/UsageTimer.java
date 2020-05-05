/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.usagestats;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

public class UsageTimer {

  private interface Clock {
    long timeNow();
  }

  private static final Clock REAL_CLOCK = () -> System.currentTimeMillis();

  private static class StopTimeClock implements Clock {
    private final long time;

    public StopTimeClock(long time) {
      this.time = time;
    }

    @Override
    public long timeNow() {
      return time;
    }
  }

  private static final ThreadLocal<Clock> CLOCK = ThreadLocal.withInitial(() -> REAL_CLOCK);

  private long timeNow() {
    return CLOCK.get().timeNow();
  }

  public static <T> T doInStopTime(long time,Callable<T> callable) throws Exception {
    try {
      CLOCK.set(new StopTimeClock(time));
      return callable.call();
    } finally {
      CLOCK.remove();
    }
  }

  private String name;
  private AtomicLong accumulatedTime;
  private int multiplier;
  private long lastStart;

  public UsageTimer() {
    accumulatedTime = new AtomicLong();
  }

  public String getName() {
    return name;
  }

  public UsageTimer setName(String name) {
    this.name = name;
    return this;
  }

  public long getAccumulatedTime() {
    changeMultiplier(0);
    return accumulatedTime.get();
  }

  public UsageTimer setAccumulatedTime(long accumulatedTime) {
    this.accumulatedTime.set(accumulatedTime);
    lastStart = timeNow();
    return this;
  }

  @JsonIgnore
  public boolean isRunning() {
    return getMultiplier() > 0;
  }

  protected int getMultiplier() {
    return multiplier;
  }

  protected synchronized UsageTimer changeMultiplier(int count) {
    long now = timeNow();
    if (lastStart > 0 && isRunning()) {
      accumulatedTime.addAndGet(getMultiplier() * (now - lastStart));
    }
    multiplier += count;
    Preconditions.checkState(getMultiplier() >= 0, Utils.formatL("UsageTimer '{}' multiplier went negative"));
    lastStart = now;
    return this;
  }

  public UsageTimer start() {
    return changeMultiplier(1);
  }

  public synchronized boolean startIfNotRunning() {
    if (isRunning()) {
      return false;
    }
    start();
    return true;
  }

  public UsageTimer stop() {
    return changeMultiplier(-1);
  }

  public synchronized boolean stopIfRunning() {
    if (!isRunning()) {
      return false;
    }
    stop();
    return true;
  }

  // returns fresh UsageTimer just reset to zero accumulated time
  public UsageTimer roll() {
    int multiplier;
    synchronized (this) {
      multiplier = getMultiplier();
      changeMultiplier(-multiplier); //stopAll;
    }
    return new UsageTimer().setName(getName()).changeMultiplier(multiplier);
  }

  public UsageTimer snapshot() {
    return new UsageTimer().setName(getName()).setAccumulatedTime(getAccumulatedTime());
  }

  @Override
  public String toString() {
    return "UsageTimer{" + "name='" + name + '\'' + ", accumulatedTime=" + accumulatedTime + '}';
  }
}
