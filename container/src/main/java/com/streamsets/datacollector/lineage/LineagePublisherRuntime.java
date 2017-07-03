/**
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
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineagePublisher;

import java.util.List;
import java.util.function.Supplier;

/**
 * Wrapper on top of LinagePublisher to execute it's methods in the proper class loader.
 */
public class LineagePublisherRuntime {

  /**
   * Definition of the publisher plugin (metadata)
   */
  private final LineagePublisherDefinition definition;

  /**
   * Instance of the publisher created in init phase.
   *
   * The class will be loaded inside the definition.getClassLoader() class loader.
   */
  private LineagePublisher publisher;

  public LineagePublisherRuntime(
    LineagePublisherDefinition definition,
    LineagePublisher lineagePublisher
  ) {
    this.definition = definition;
    this.publisher = lineagePublisher;
  }

  public List<LineagePublisher.ConfigIssue> init(LineagePublisherContext context) {
    return withClassLoader(() -> publisher.init(context));
  }

  public boolean publishEvents(List<LineageEvent> events) {
    return withClassLoader(() -> publisher.publishEvents(events));
  }

  public void destroy() {
    withClassLoader(() -> { publisher.destroy(); return null; });
  }

  /**
   * Run given consumer inside publisher's class loader.
   *
   * @param supplier Lambda function that needs to be executed inside the class loader.
   */
  private<T> T withClassLoader(Supplier<T> supplier) {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(definition.getClassLoader());
      return supplier.get();
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

}
