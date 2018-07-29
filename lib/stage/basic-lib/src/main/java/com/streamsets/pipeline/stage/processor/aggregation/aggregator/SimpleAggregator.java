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
package com.streamsets.pipeline.stage.processor.aggregation.aggregator;

/**
 * Base class for simple Aggregator classes.
 */
public abstract class SimpleAggregator<A extends SimpleAggregator, T> extends Aggregator<A, T> {

  /**
   * Constructor
   *  @param type type of the aggregator value.
   * @param name name of the aggregator.
   */
  protected SimpleAggregator(Class<? extends Number> type, String name) {
    super(type, name);
  }

  /**
   * Processes the given value into the aggregator.
   *
   * @param value value to process.
   */
  public void process(T value) {
    getData().process(value);
  }

}
