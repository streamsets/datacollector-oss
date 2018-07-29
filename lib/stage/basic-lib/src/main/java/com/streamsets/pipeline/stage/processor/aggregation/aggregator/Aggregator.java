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
 * Base class for all aggregator functions.
 * <p/>
 * Aggregators do not hold their aggregated data, they create the data structure to hold it, via the
 * {@link #createAggregatorData(long)}, but this data structure is provided by the {@link #getDataProvider()}.
 * This allows atomic bulk replacement of the data structure for a set of Aggregators, such as all the Aggregators of
 * an AggregatorProcessor and the groupBy element Aggregators of a GroupByAggregator.
 * <p/>
 * Aggregator instances must be created via the {@link Aggregators} class which is responsible for providing the
 * AggregatorDataProvider to the Aggregator.
 *
 * @param <A> aggregator concrete class.
 * @param <T> valueType (a Number subclass) of the aggregated value.
 */
public abstract class Aggregator<A extends Aggregator, T> {

  /**
   * Interface that defines data from the aggregator function that can be aggregated by another Aggregator.
   * <p/>
   * Through this interface correlated values obtained in different instances can be aggregated by an Aggregator
   * into a total aggregated value.
   *
   * @param <A> aggregator valueType name.
   */
  public interface Aggregatable<A> {

    default String getType() {
      return getClass().getSimpleName();
    }

    String getName();
  }

  private final String name;
  private final Class<? extends Number> valueType;
  private AggregatorDataProvider dataProvider;

  /**
   * Creates an Aggregator.
   *  @param valueType type of the aggregated value.
   * @param name aggregator name.
   */
  public Aggregator(Class<? extends Number> valueType, String name) {
    this.valueType = valueType;
    this.name = name;
  }

  /**
   * Returns the numeric type this aggregator aggregates.
   *
   * @return the numeric type this aggregator aggregates.
   */
  public Class<? extends Number> getValueType() {
    return valueType;
  }

  /**
   * Returns the name of the aggregator.
   * @return the name of the aggregator.
   */
  public String getName() {
    return name;
  }

  /**
   * Creates an AggregatorData for the Aggregator.
   * <p/>
   * This method is used by the AggregatorDataProvider to create a new AggregatorData instance when performing a
   * window change.
   *
   * @return a new AggregatorData instance.
   * @param timeWindowMillis
   */
  abstract AggregatorData createAggregatorData(long timeWindowMillis);

  /**
   * Sets the DataProvider for the Aggregator.
   *
   * @param dataProvider the DataProvider to set.
   */
  void setDataProvider(AggregatorDataProvider dataProvider) {
    this.dataProvider = dataProvider;
  }

  /**
   * Returns the DataProvider for the Aggregator of the Aggregator.
   *
   * @return the DataProvider for the Aggregator.
   */
  AggregatorDataProvider getDataProvider() {
    return dataProvider;
  }

  /**
   * Returns the current AggregatorData of the Aggregator.
   * <p/>
   * The AggregatorData instance is provided by the DataProvider of the Aggregator.
   *
   * @return the current AggregatorData of the Aggregator.
   */
  @SuppressWarnings("unchecked")
  protected AggregatorData<A, T> getData() {
    return getDataProvider().getData(this);
  }

  /**
   * Returns the current computed value of the Aggregator.
   * <p/>
   * This method delegates to the {@link AggregatorData#get()}.
   *
   * @return the current computed value of the Aggregator.
   */
  public T get() {
    return getData().get();
  }

  /**
   * Returns the current aggregatable value of the aggregator.
   * <p/>
   * Aggregatable values of correlated aggregators can be consolidated to provide a total aggregation value.
   * <p/>
   * This method delegates to the {@link AggregatorData#getAggregatable()}.
   *
   * @return the current aggregatable value of the aggregator.
   */
  public Aggregatable<A> getAggregatable() {
    return getData().getAggregatable();
  }

  /**
   * Aggregates an aggregatable value to this aggregator.
   * <p/>
   * And aggregator using this method is consolidating aggregation values of correlated aggregators.
   * <p/>
   * This method delegates to the {@link AggregatorData#aggregate(Aggregatable)} ()}.
   *
   * @param aggregatable the aggregatable value to aggregate.
   */
  public void aggregate(Aggregatable<A> aggregatable) {
    getData().aggregate(aggregatable);
  }

  /**
   * Returns the String representation of the Aggregator.
   *
   * @return the String representation of the Aggregator.
   */
  @Override
  public String toString() {
    Object value = (getDataProvider() == null || getData() == null) ? "null" : getData().get();
    return getClass().getSimpleName() + "{" + "name='" + getName() + "\', current=" + value + '}';
  }

  public String toStringObject() {
    return super.toString();
  }
}
