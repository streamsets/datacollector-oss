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

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An Aggregators groups a set of Aggregators with a common DataWindow. Their values are rolled atomically.
 * <p/>
 * It creates simple and group-by aggregators, all aggregators must be defined before starting the Aggregators instance.
 */
public class Aggregators {

  private static final Set<Class<? extends SimpleAggregator>> AGGREGATOR_CLASSES = ImmutableSet.of(
      CountAggregator.class,
      DoubleAvgAggregator.class,
      LongAvgAggregator.class,
      DoubleMinAggregator.class,
      LongMinAggregator.class,
      DoubleMaxAggregator.class,
      LongMaxAggregator.class,
      DoubleStdDevAggregator.class,
      DoubleSumAggregator.class,
      LongSumAggregator.class
  );

  private static final Map<Class<? extends SimpleAggregator>, Constructor<? extends SimpleAggregator>> CONSTRUCTORS =
      new HashMap<>();

  static Constructor<? extends SimpleAggregator> getConstructor(Class<? extends SimpleAggregator> klass) {
    try {
      return klass.getConstructor(String.class, GroupByAggregator.class);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static {
    for (Class<? extends SimpleAggregator> klass : AGGREGATOR_CLASSES) {
      CONSTRUCTORS.put(klass, getConstructor(klass));
    }
  }

  private final AggregatorDataProvider dataProvider;
  private boolean started;
  private boolean stopped;

  /**
   * Creates an Aggregators.
   *
   * @param windowsToKeep datawindows to remember, including the active one.
   */
  public Aggregators(int windowsToKeep) {
    dataProvider = new AggregatorDataProvider(windowsToKeep);
  }

  /**
   * Retruns the Aggregators AggregatorDataProvider that backs all aggregators of the Aggregators instance.
   *
   * @return the Aggregators AggregatorDataProvider that backs all aggregators of the Aggregators instance.
   */
  public AggregatorDataProvider getDataProvider() {
    return dataProvider;
  }

  /**
   * Creates a simple Aggregator.
   *
   * @param name name of the aggregator.
   * @param klass Aggregator class
   * @return a simple aggregator instance.
   */
  @SuppressWarnings("unchecked")
  public <A extends SimpleAggregator> A createSimple(String name, Class<? extends Aggregator> klass) {
    return createSimple(name, (Class<A>) klass, null);
  }

  @SuppressWarnings("unchecked")
  <A extends SimpleAggregator, T> A createSimple(String name, Class<A> klass, GroupByAggregator<A, T> groupByParent) {
    Utils.checkState(!started || groupByParent != null, "Already started");
    try {
      A aggregator = (A) CONSTRUCTORS.get(klass).newInstance(name, groupByParent);
      dataProvider.addAggregator(aggregator);
      aggregator.setDataProvider(dataProvider);
      return aggregator;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Creates a group-by Agregator.
   *
   * @param name name of the group-by Aggregator.
   * @param aKlass simple aggregator class backing each group-by element.
   * @return a group-by aggregator.
   */
  @SuppressWarnings("unchecked")
  public <A extends SimpleAggregator, N extends Number> GroupByAggregator<A, N> createGroupBy(
      String name, Class<? extends Aggregator> aKlass
  ) {
    Utils.checkState(!started, "Already started");
    GroupByAggregator<A, N> aggregator = new GroupByAggregator(name, aKlass, this);
    dataProvider.addAggregator(aggregator);
    aggregator.setDataProvider(dataProvider);
    return aggregator;
  }

  /**
   * Starts the Aggregators instance.
   *
   * @param newDataWindowEndTimeMillis ending time of the new DataWindow.
   */
  public void start(long newDataWindowEndTimeMillis) {
    Utils.checkState(!started, "Already started");
    Utils.checkState(!stopped, "Already stopped");
    dataProvider.start(newDataWindowEndTimeMillis);
    started = true;
  }

  /**
   * Stops the Aggregators instance.
   */
  public void stop() {
    Utils.checkState(started, "Already started");
    Utils.checkState(!stopped, "Already stopped");
    dataProvider.stop();
    stopped = true;
  }

  /**
   * Atomically rolls the DataWindow of all aggregators associated with the Aggregators instance.
   *
   * @param newDataWindowEndTimeMillis ending time of the new DataWindow.
   * @return a Map with all the aggregators data of the DataWindow that closed.
   */
  public Map<Aggregator, AggregatorData> roll(long newDataWindowEndTimeMillis) {
    Utils.checkState(started, "Not started");
    Utils.checkState(!stopped, "Already stopped");
    return dataProvider.roll(newDataWindowEndTimeMillis);
  }

}
