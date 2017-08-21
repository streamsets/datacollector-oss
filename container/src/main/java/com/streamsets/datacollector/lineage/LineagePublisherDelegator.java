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
package com.streamsets.datacollector.lineage;

import com.streamsets.pipeline.api.lineage.LineageEvent;

import java.util.List;

/**
 * Simple interface to delegate call for publishing lineage events.
 */
public interface LineagePublisherDelegator {

  /**
   * List delegator that will save lineage events in a list.
   *
   * Particularly useful for Runners (test, embedded stages, ...).
   */
  public static class ListDelegator implements LineagePublisherDelegator {

    private final List<LineageEvent> events;

    public ListDelegator(List<LineageEvent> events) {
      this.events = events;
    }

    @Override
    public void publishLineageEvent(LineageEvent event) {
      this.events.add(event);
    }
  }

  /**
   * Task delegator that will delegate to main LineagePublisherTask.
   *
   * Particularly useful for main data collector processing.
   */
  public static class TaskDelegator implements LineagePublisherDelegator {

    private final LineagePublisherTask task;

    public TaskDelegator(LineagePublisherTask task) {
      this.task = task;
    }

    @Override
    public void publishLineageEvent(LineageEvent event) {
      this.task.publishEvent(event);
    }
  }

  /**
   * No-op delegator tha will discard all events.
   *
   * Particularly useful for testing.
   */
  public static class NoopDelegator implements LineagePublisherDelegator {

    @Override
    public void publishLineageEvent(LineageEvent event) {
      // Noop
    }
  }

  /**
   * Publish given lineage event to the delegated system
   */
  public void publishLineageEvent(LineageEvent event);
}
