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

import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineagePublisher;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class TestLineagePublisherTaskImpl {

  /**
   * Test publisher class that can validate what is the task implementation doing.
   */
  public static class TestLineagePublisher implements LineagePublisher {
    boolean initCalled = false;
    boolean destroyCalled = false;
    List<LineageEvent> events = new ArrayList<>();

    @Override
    public List<ConfigIssue> init(Context context) {
      initCalled = true;
      return Collections.emptyList();
    }

    @Override
    public boolean publishEvents(List<LineageEvent> events) {
      this.events.addAll(events);
      return true;
    }

    @Override
    public void destroy() {
      destroyCalled = true;
    }
  }

  private LineagePublisherTaskImpl lineagePublisherTask;

  @Before
  public void setUp() {
    Configuration configuration = new Configuration();
    configuration.set(LineagePublisherConstants.CONFIG_LINEAGE_PUBLISHERS, "test");
    configuration.set(LineagePublisherConstants.configDef("test"), "test_library::test_publisher");

    StageLibraryDefinition libraryDefinition = mock(StageLibraryDefinition.class);
    when(libraryDefinition.getName()).thenReturn("Test library");

    LineagePublisherDefinition def = new LineagePublisherDefinition(
      libraryDefinition,
      Thread.currentThread().getContextClassLoader(),
      TestLineagePublisher.class,
      "test",
      "test",
      "test"
    );

    StageLibraryTask stageLibraryTask = mock(StageLibraryTask.class);
    when(stageLibraryTask.getLineagePublisherDefinition(anyString(), anyString())).thenReturn(def);

    lineagePublisherTask = new LineagePublisherTaskImpl(configuration, stageLibraryTask);
  }

  @Test
  public void testRuntimeLifecycle() {
    lineagePublisherTask.initTask();
    assertTrue(((TestLineagePublisher)lineagePublisherTask.publisherRuntime.publisher).initCalled);
    assertFalse(((TestLineagePublisher)lineagePublisherTask.publisherRuntime.publisher).destroyCalled);

    lineagePublisherTask.stopTask();
    assertTrue(((TestLineagePublisher)lineagePublisherTask.publisherRuntime.publisher).destroyCalled);
  }

  @Test
  public void testConsumeEvents() {
    lineagePublisherTask.initTask();
    lineagePublisherTask.runTask();

    // Publish 10 events
    for(int i = 0; i < 10; i++) {
      LineageEvent event = mock(LineageEvent.class);
      lineagePublisherTask.publishEvent(event);
    }

    TestLineagePublisher publisher = (TestLineagePublisher)lineagePublisherTask.publisherRuntime.publisher;
    await().until(() -> publisher.events.size() == 10);

    lineagePublisherTask.stopTask();
  }

}
