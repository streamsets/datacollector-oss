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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineagePublisher;
import com.streamsets.pipeline.api.lineage.LineagePublisherDef;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

public class TestLineagePublisherDefinitionExtractor {

  @LineagePublisherDef(
    label = "Awesome publisher",
    description = "Solves all your problems."
  )
  public static class ExampleLineagePublisher implements LineagePublisher {

    @Override
    public List<ConfigIssue> init(Context context) {
      return null;
    }

    @Override
    public boolean publishEvents(List<LineageEvent> events) {
      return false;
    }

    @Override
    public void destroy() {

    }
  }

  private static final StageLibraryDefinition MOCK_LIB_DEF = new StageLibraryDefinition(
    TestLineagePublisherDefinitionExtractor.class.getClassLoader(),
    "mock",
    "MOCK",
    new Properties(),
    null,
    null,
    null
  );

  @Test
  public void testBasicExtraction() {
    // Extract simple definition
    LineagePublisherDefinition definition = LineagePublisherDefinitionExtractor.get().extract(
      MOCK_LIB_DEF,
      ExampleLineagePublisher.class
    );

    Assert.assertNotNull(definition);
    Assert.assertEquals(getClass().getClassLoader(), definition.getClassLoader());
    Assert.assertEquals(ExampleLineagePublisher.class, definition.getKlass());
    Assert.assertEquals("Awesome publisher", definition.getLabel());
    Assert.assertEquals("Solves all your problems.", definition.getDescription());
  }

}
