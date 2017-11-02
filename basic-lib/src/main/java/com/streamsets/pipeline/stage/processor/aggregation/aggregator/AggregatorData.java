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
 * The AggregatorData holds the aggregated data for an Aggregator, it is responsible for updating it.
 * <p/>
 * Each Aggregator has its own AggregatorData implementation.
 *
 * @param <A> aggregator concrete class.
 * @param <T> valueType (a Number subclass) of the aggregated value.
 */
public abstract class AggregatorData<A extends Aggregator, T> {
  private final String name;
  private long time;

  public AggregatorData(String name, long time) {
    this.name = name;
    this.time = time;
  }

  /**
   * Returns the Aggregator name.
   *
   * @return the Aggregator name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the time window of the data.
   *
   * @return the time window of the data, in Unix epoch milliseconds.
   */
  public long getTime() {
    return time;
  }

  /**
   * Sets the time window of the data.
   *
   * @param time
   */
  public void setTime(long time) {
    this.time = time;
  }

  /**
   * Processes the given value into the aggregator accordingly the the aggregation function.
   *
   * @param value the value to aggregate.
   */
  public abstract void process(T value);

  /**
   * Returns the current aggregation value of the aggregator.
   *
   * @return the current aggregation value of the aggregator.
   */
  public abstract T get();

  /**
   * Returns the current aggregatable value of the aggregator.
   * <p/>
   * Aggregatable values of correlated aggregators can be consolidated to provide a total aggregation value.
   *
   * @return the current aggregatable value of the aggregator.
   */
  public abstract Aggregator.Aggregatable<A> getAggregatable();

  /**
   * Aggregates an aggregatable value to this aggregator.
   * <p/>
   * And aggregator using this method is consolidating aggregation values of correlated aggregators.
   *
   * @param aggregatable the aggregatable value to aggregate.
   */
  public abstract void aggregate(Aggregator.Aggregatable<A> aggregatable);

}
