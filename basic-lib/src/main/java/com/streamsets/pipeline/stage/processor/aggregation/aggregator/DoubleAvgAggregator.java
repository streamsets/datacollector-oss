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

import com.streamsets.pipeline.api.impl.Utils;

/**
 * Double Average Aggregator.
 */
public class DoubleAvgAggregator extends SimpleAggregator<DoubleAvgAggregator, Double> {

  public static class DoubleAvgAggregatable implements Aggregatable<DoubleAvgAggregator> {
    private String name;
    private long count;
    private double total;
    private double average;

    @Override
    public String getName() {
      return name;
    }

    public DoubleAvgAggregatable setName(String name) {
      this.name = name;
      return this;
    }

    public long getCount() {
      return count;
    }

    public DoubleAvgAggregatable setCount(long count) {
      this.count = count;
      return this;
    }

    public double getTotal() {
      return total;
    }

    public DoubleAvgAggregatable setTotal(double total) {
      this.total = total;
      return this;
    }

    public double getAverage() {
      return average;
    }

    public DoubleAvgAggregatable setAverage(double average) {
      this.average = average;
      return this;
    }
  }

  private class Data extends AggregatorData<DoubleAvgAggregator, Double> {
    private long count;
    private double total;

    public Data(String name, long time) {
      super(name, time);
    }

    @Override
    public String getName() {
      return DoubleAvgAggregator.this.getName();
    }

    @Override
    public void process(Double value) {
      if (value != null) {
        synchronized (this) {
          total += value;
          count++;
        }
      }
    }

    @Override
    public synchronized Double get() {
      return (count == 0) ? null : total / count;
    }

    @Override
    public Aggregatable<DoubleAvgAggregator> getAggregatable() {
      DoubleAvgAggregatable aggregatable = new DoubleAvgAggregatable().setName(getName());
      synchronized (this) {
        aggregatable.setCount(count).setTotal(total).setAverage(get());
      }
      return aggregatable;
    }

    @Override
    public void aggregate(Aggregatable<DoubleAvgAggregator> aggregatable) {
      Utils.checkNotNull(aggregatable, "aggregatable");
      Utils.checkArgument(
          getName().equals(aggregatable.getName()),
          Utils.formatL("Aggregable '{}' does not match this aggregation '{}", aggregatable.getName(), getName())
      );
      Utils.checkArgument(aggregatable instanceof DoubleAvgAggregatable, Utils.formatL(
          "Aggregatable '{}' is a '{}' it should be '{}'",
          getName(),
          aggregatable.getClass().getSimpleName(),
          DoubleAvgAggregatable.class.getSimpleName()
      ));
      synchronized (this) {
        count += ((DoubleAvgAggregatable) aggregatable).getCount();
        total += ((DoubleAvgAggregatable) aggregatable).getTotal();
      }
    }
  }

  public DoubleAvgAggregator(String name) {
    super(Double.class, name);
  }

  @Override
  public AggregatorData createAggregatorData(long timeWindowMillis) {
    return new Data(getName(), timeWindowMillis);
  }

}
